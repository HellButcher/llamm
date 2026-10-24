use std::{borrow::Cow, pin::pin, sync::Arc};

use crossterm::event::{Event, EventStream, KeyCode};
use futures::StreamExt;
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, Layout},
    prelude::{Buffer, Rect},
    style::{Color, Style, Stylize},
    text::Line,
    widgets::{Block, HighlightSpacing, Row, StatefulWidget, Table, TableState, Widget},
};
use tracing::error;

use crate::api::{Client, Model, ModelInfo, ModelStatus, ModelUpdates};

pub struct App {
    should_quit: bool,
    models: ModelListWidget,
}

impl App {
    pub fn new() -> Self {
        Self {
            should_quit: false,
            models: ModelListWidget::default(),
        }
    }

    pub async fn run(
        mut self,
        mut terminal: DefaultTerminal,
        client: Arc<Client>,
    ) -> anyhow::Result<()> {
        let mut events = EventStream::new();
        let mut updates = pin!(client.models_with_updates()?);
        self.models.loading_state = LoadingState::Loading;
        while !self.should_quit {
            terminal.draw(|frame| self.render(frame))?;

            tokio::select! {
                Some(event) = events.next() => {
                   self.handle_event(event?, &client);
                }

                Some(update) = updates.next() => match update {
                    Ok(ModelUpdates::Initial(mut initial)) => {
                        sort_models(&mut initial);
                        self.models.models = initial;
                        self.models.loading_state = LoadingState::Loaded;
                    }
                    Ok(ModelUpdates::Update(update)) => {
                        update.apply_to(&mut self.models.models);
                        sort_models(&mut self.models.models);
                    }
                    Err(e) => {
                        error!("Error receiving model updates: {:?}", e);
                        self.models.loading_state = LoadingState::Error(format!("{:?}", e));
                    }
                },

                else => break,
            }
        }
        Ok(())
    }

    pub fn render(&mut self, frame: &mut Frame) {
        let layout = Layout::vertical([
            Constraint::Length(1), // Title
            Constraint::Fill(1),   // Table
            Constraint::Length(1), // Legend
        ]);
        let [title_area, body_area, legend_area] = frame.area().layout(&layout);

        let title = Line::from("llamm — Model Manager").centered().bold();
        frame.render_widget(title, title_area);
        frame.render_widget(&mut self.models, body_area);

        let legend = Line::from("✅ Loaded  ⏳ Loading  📥 Downloading  💤 Sleeping  ⚪ Unloaded  ❌ Failed | 🖼️ Image  🎵 Audio  📹 Video  📝 Text")
            .centered()
            .style(Style::default().fg(Color::Gray));
        frame.render_widget(legend, legend_area);
    }

    fn handle_event(&mut self, event: crossterm::event::Event, client: &Arc<Client>) {
        if let Event::Key(key) = event {
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => {
                    self.should_quit = true;
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.models.select_previous();
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.models.select_next();
                }
                KeyCode::Enter | KeyCode::Char('l') => {
                    if let Some(selected) = self.models.selected_model() {
                        let id = Cow::Owned(selected.info.id.clone());
                        let client = Arc::clone(client);
                        tokio::spawn(async move { client.load_model(id).await });
                    }
                }
                KeyCode::Delete | KeyCode::Char('u') => {
                    if let Some(selected) = self.models.selected_model() {
                        let id = Cow::Owned(selected.info.id.clone());
                        let client = Arc::clone(client);
                        tokio::spawn(async move { client.unload_model(id).await });
                    }
                }
                _ => {}
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
struct ModelListWidget {
    models: Vec<Model>,
    table_state: TableState,
    loading_state: LoadingState,
}

impl ModelListWidget {
    pub fn select_previous(&mut self) {
        self.table_state.select_previous();
    }
    pub fn select_next(&mut self) {
        self.table_state.select_next();
    }
    pub fn selected_model(&self) -> Option<&Model> {
        self.table_state.selected().and_then(|i| self.models.get(i))
    }
}

impl Widget for &mut ModelListWidget {
    fn render(self, area: Rect, buf: &mut Buffer)
    where
        Self: Sized,
    {
        let loading_state = Line::from(format!("{:?}", self.loading_state)).right_aligned();
        let block = Block::bordered()
            .title("Available Models")
            .title(loading_state)
            .title_bottom("j/k to scroll, q to quit, l to load, u to unload");
        // a table with the list of models
        let rows = self.models.iter();
        let widths = [
            Constraint::Length(5),
            Constraint::Fill(1),
            Constraint::Max(49),
        ];
        let table = Table::new(rows, widths)
            .block(block)
            .highlight_spacing(HighlightSpacing::Always)
            .highlight_symbol(">>")
            .row_highlight_style(Style::new().on_blue());

        StatefulWidget::render(table, area, buf, &mut self.table_state);
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
enum LoadingState {
    #[default]
    Idle,
    Loading,
    Loaded,
    Error(String),
}

impl<'a> From<&'a Model> for Row<'a> {
    fn from(value: &'a Model) -> Self {
        Row::new(vec![
            Cow::Borrowed(value.status.icon()),
            Cow::Borrowed(value.info.display_name()),
            Cow::Owned(value.info.modality_icons()),
        ])
    }
}

/// Sort models: loaded first, then sleeping, then loading, then alphabetical.
fn sort_models(models: &mut [Model]) {
    models.sort_by(|a, b| {
        a.status
            .rank()
            .cmp(&b.status.rank())
            .then_with(|| a.info.id.cmp(&b.info.id))
    });
}

impl ModelInfo {
    /// Display name: first alias or the model id.
    pub fn display_name(&self) -> &str {
        if let Some(alias) = self.aliases.first() {
            alias
        } else {
            &self.id
        }
    }

    pub fn modality_icons(&self) -> String {
        let mut icons = String::new();
        if let Some(mods) = self
            .architecture
            .as_ref()
            .map(|a| a.input_modalities.as_slice())
        {
            for m in mods {
                match m.as_str() {
                    "image" => icons.push_str("🖼️"),
                    "audio" => icons.push('🎵'),
                    "video" => icons.push('📹'),
                    "text" => {} // ignored
                    _ => icons.push('❓'),
                }
            }
        }
        icons
    }
}

#[cfg(false)]
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

impl ModelStatus {
    fn rank(&self) -> u8 {
        match self.value.as_str() {
            "loaded" => 0,
            "sleeping" => 1,
            "loading" => 2,
            "downloading" => 3,
            "unloaded" if self.is_failed() => 4,
            "unloaded" => 5,
            _ => 6,
        }
    }

    pub fn icon(&self) -> &'static str {
        match self.value.as_str() {
            "unloaded" if self.is_failed() => "❌",
            "loaded" => "✅",
            "loading" => "⏳",
            "downloading" => "📥",
            "sleeping" => "💤",
            "unloaded" => "⚪",
            _ => "❓",
        }
    }

    pub fn status_label(&self) -> &str {
        match self.value.as_str() {
            "unloaded" if self.is_failed() => "failed",
            "loaded" => "loaded",
            "loading" => "loading…",
            "downloading" => "downloading…",
            "sleeping" => "sleeping",
            "unloaded" => "unloaded",
            _ => self.value.as_str(),
        }
    }
}
