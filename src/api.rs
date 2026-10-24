use futures::{TryStreamExt, stream::FusedStream};
use reqwest_eventsource::{Event, RequestBuilderExt};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    task::Poll::{self, Ready},
};
use tracing::info;

// ── Data models ──────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct ModelList {
    pub data: Vec<Model>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Model {
    pub status: ModelStatus,
    #[serde(flatten)]
    pub info: ModelInfo,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ModelInfo {
    pub id: String,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub architecture: Option<Architecture>,
    pub meta: Option<ModelMeta>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ModelStatus {
    pub value: String,
    pub exit_code: Option<u32>,
}

impl ModelStatus {
    pub(crate) fn is_failed(&self) -> bool {
        matches!(self.exit_code, Some(code) if code != 0)
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct Architecture {
    pub input_modalities: Vec<String>,
    pub output_modalities: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ModelMeta {
    pub size: Option<u64>,
    pub n_ctx: Option<u64>,
    pub n_params: Option<u64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ModelSSE {
    pub model: String,
    #[serde(flatten)]
    pub event: ModelEvent,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "event", content = "data")]
pub enum ModelEvent {
    #[serde(rename = "model_status", alias = "status_change")]
    ModelStatus {
        status: String,
        #[serde(default)]
        exit_code: Option<u32>,
        #[serde(default)]
        info: Option<ModelInfo>,
    },
    #[cfg(false)]
    #[serde(rename = "download_progress")]
    DownloadProgress(HashMap<String, DownloadProgress>),
    #[serde(rename = "model_remove")]
    ModelRemove,
    #[serde(rename = "models_reload")]
    ModelsReload,
    #[serde(other)]
    Unknown,
}

#[cfg(false)]
#[derive(Debug, Deserialize, Clone)]
pub struct DownloadProgress {
    pub done: u64,
    pub total: u64,
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
        info!("Fetching model list from {}/models", self.base_url);
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
        info!("Fetched {} models", list.data.len());
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
                                update.apply_to(buffered);
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
                    info!("Connected to model updates SSE stream");
                    let models = self.models().await?;
                    Ok(ModelUpdates::Initial(models))
                }
                Event::Message(message) => {
                    let model_sse: ModelSSE = serde_json::from_str(&message.data)?;
                    info!("Received model update: {:?}", model_sse.model);
                    Ok(ModelUpdates::Update(model_sse))
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
        info!("Loading model: {}", model_id);
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
        info!("Unloading model: {}", model_id);
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
    Update(ModelSSE),
}

impl ModelSSE {
    pub fn apply_to(self, dest: &mut Vec<Model>) -> bool {
        let model_id = self.model;
        match self.event {
            ModelEvent::ModelStatus {
                status,
                exit_code,
                info,
            } => {
                if let Some(model) = dest.iter_mut().find(|m| m.info.id == model_id) {
                    model.status.value = status;
                    model.status.exit_code = exit_code;
                    if let Some(info) = info {
                        model.info.merge(info);
                    }
                    true
                } else {
                    false
                }
            }
            ModelEvent::ModelRemove => {
                dest.retain(|m| m.info.id != model_id);
                true
            }
            _ => {
                // For other events, we don't modify the model list
                false
            }
        }
    }
}

impl ModelInfo {
    pub fn merge(&mut self, other: ModelInfo) {
        fn merge_string_vec(dest: &mut Vec<String>, src: Vec<String>) {
            for item in src {
                if !dest.contains(&item) {
                    dest.push(item);
                }
            }
        }

        merge_string_vec(&mut self.aliases, other.aliases);
        merge_string_vec(&mut self.tags, other.tags);
        if let Some(arch) = other.architecture {
            self.architecture = Some(arch);
        }
        if let Some(meta) = other.meta {
            self.meta = Some(meta);
        }
    }
}
