use futures::{StreamExt, TryStreamExt, poll, stream::FusedStream};
use reqwest_eventsource::{Event, RequestBuilderExt};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    task::{
        Poll::{self, Ready},
        ready,
    },
};

// ── Data models ──────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct ModelList {
    pub data: Vec<Model>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Model {
    pub id: String,
    pub status: ModelStatus,
    pub architecture: Option<Architecture>,
    #[serde(default)]
    pub aliases: Vec<String>,
    pub meta: Option<ModelMeta>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ModelStatus {
    pub value: String,
    #[serde(default)]
    pub failed: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Architecture {
    pub input_modalities: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ModelMeta {
    pub size: Option<u64>,
}

#[derive(Debug, Serialize, Clone)]
pub struct ModelRequest<'a> {
    pub model: &'a str,
}

// ── API client ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Client {
    base_url: String,
    http: reqwest::Client,
}

impl Client {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.to_string(),
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("failed to build http client"),
        }
    }

    /// Fetch the full model list (GET /models).
    pub async fn models(&self) -> anyhow::Result<Vec<Model>> {
        let resp = self
            .http
            .get(format!("{}/models", self.base_url))
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            return Err(anyhow::anyhow!("GET /models returned {status}"));
        }

        let list: ModelList = resp.json().await?;
        Ok(list.data)
    }

    /// Fetch the full model list and subscribe to updates, returning both the initial list and a stream of updates.
    pub fn models_with_updates(
        &self,
    ) -> anyhow::Result<impl futures::Stream<Item = anyhow::Result<ModelUpdates>>> {
        pin_project_lite::pin_project! {
            /// A stream that integrates a sequence of Ready Updates into a single Initial update, and then continues to yield Update events.
            struct SaturateInitial<S> {
                #[pin]
                stream: S,
                buffered: Option<Vec<Model>>,
                last_err: Option<anyhow::Error>,
                done: bool,
            }
        }

        impl<S> futures::Stream for SaturateInitial<S>
        where
            S: futures::Stream<Item = anyhow::Result<ModelUpdates>>,
        {
            type Item = anyhow::Result<ModelUpdates>;

            fn poll_next(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Option<Self::Item>> {
                let mut this = self.as_mut().project();
                if let Some(err) = this.last_err.take() {
                    return Ready(Some(Err(err)));
                }
                if *this.done {
                    return Ready(None);
                }
                loop {
                    match this.stream.as_mut().poll_next(cx) {
                        Poll::Pending => {
                            if let Some(buffered) = this.buffered.take() {
                                return Ready(Some(Ok(ModelUpdates::Initial(buffered))));
                            } else {
                                return Poll::Pending;
                            }
                        }
                        Poll::Ready(None) => {
                            *this.done = true;
                            return Ready(
                                this.buffered
                                    .take()
                                    .map(|buffered| Ok(ModelUpdates::Initial(buffered))),
                            );
                        }
                        Poll::Ready(Some(Err(e))) => {
                            if let Some(buffered) = this.buffered.take() {
                                this.last_err.replace(e);
                                return Ready(Some(Ok(ModelUpdates::Initial(buffered))));
                            } else {
                                return Ready(Some(Err(e)));
                            }
                        }
                        Poll::Ready(Some(Ok(ModelUpdates::Initial(models)))) => {
                            *this.buffered = Some(models);
                        }
                        Poll::Ready(Some(Ok(ModelUpdates::Update(update)))) => {
                            if let Some(buffered) = this.buffered {
                                update.insert_to(buffered);
                            } else {
                                return Ready(Some(Ok(ModelUpdates::Update(update))));
                            }
                        }
                    }
                }
            }
        }

        impl<S> FusedStream for SaturateInitial<S>
        where
            S: futures::Stream<Item = anyhow::Result<ModelUpdates>>,
        {
            fn is_terminated(&self) -> bool {
                self.done
            }
        }

        let stream = self
            .http
            .get(format!("{}/models/sse", self.base_url))
            .eventsource()?
            .err_into()
            .and_then(async |event: Event| match event {
                Event::Open => {
                    let models = self.models().await?;
                    Ok(ModelUpdates::Initial(models))
                }
                Event::Message(message) => {
                    let model: Model = serde_json::from_str(&message.data)?;
                    Ok(ModelUpdates::Update(model))
                }
            });
        Ok(SaturateInitial {
            stream,
            buffered: None,
            last_err: None,
            done: false,
        })
    }

    /// Load a model by id (POST /models/load).
    pub async fn load_model(&self, model_id: Cow<'_, str>) -> anyhow::Result<()> {
        let resp = self
            .http
            .post(format!("{}/models/load", self.base_url))
            .json(&ModelRequest {
                model: model_id.as_ref(),
            })
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("POST /models/load failed {status}: {body}"));
        }
        Ok(())
    }

    /// Unload a model by id (POST /models/unload).
    pub async fn unload_model(&self, model_id: Cow<'_, str>) -> anyhow::Result<()> {
        let resp = self
            .http
            .post(format!("{}/models/unload", self.base_url))
            .json(&ModelRequest {
                model: model_id.as_ref(),
            })
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "POST /models/unload failed {status}: {body}"
            ));
        }
        Ok(())
    }
}

// ── Model helpers ────────────────────────────────────────────────────────────

pub enum ModelUpdates {
    Initial(Vec<Model>),
    Update(Model),
}

impl Model {
    /// Display name: first alias or the model id.
    pub fn display_name(&self) -> &str {
        if let Some(alias) = self.aliases.first() {
            alias
        } else {
            &self.id
        }
    }

    pub fn status_label(&self) -> &str {
        match (self.status.value.as_str(), self.status.failed) {
            ("unloaded", true) => "failed",
            ("loaded", _) => "loaded",
            ("loading", _) => "loading…",
            ("downloading", _) => "downloading…",
            ("sleeping", _) => "sleeping",
            ("unloaded", _) => "unloaded",
            _ => self.status.value.as_str(),
        }
    }

    pub fn human_size(&self) -> String {
        self.meta
            .as_ref()
            .and_then(|m| m.size)
            .map(human_size)
            .unwrap_or_else(|| "?".to_string())
    }

    /// Returns a modality icon based on input modalities.
    pub fn modality_icon(&self) -> &'static str {
        let mods = self
            .architecture
            .as_ref()
            .map(|a| a.input_modalities.as_slice())
            .unwrap_or(&DEFAULT_MODS[..]);

        if mods.iter().any(|m| m == "image") && mods.iter().any(|m| m == "audio") {
            "🎥"
        } else if mods.iter().any(|m| m == "image") {
            "🖼️"
        } else if mods.iter().any(|m| m == "audio") {
            "🎵"
        } else if mods.iter().any(|m| m == "video") {
            "📹"
        } else {
            "📝"
        }
    }

    pub fn insert_to(self, other: &mut Vec<Model>) {
        if let Some(sel) = other.iter_mut().find(|m| m.id == self.id) {
            *sel = self;
        } else {
            other.push(self);
        }
    }
}

const DEFAULT_MODS: &[String] = &[];

fn human_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    if bytes == 0 {
        return "0 B".to_string();
    }
    let idx = ((bytes as f64).ln() / 1024.0_f64.ln()).floor() as usize;
    let idx = idx.min(UNITS.len() - 1);
    let val = bytes as f64 / 1024f64.powi(idx as i32);
    format!("{val:.1} {}", UNITS[idx])
}
