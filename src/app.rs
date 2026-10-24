use std::{
    borrow::Cow,
    pin::{Pin, pin},
    sync::Arc,
    task::{Poll, ready},
};

use crossterm::event::{Event, EventStream, KeyCode};
use futures::{FutureExt, Stream, StreamExt, TryStream, TryStreamExt, stream::FusedStream};
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, Layout},
    prelude::{Buffer, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, HighlightSpacing, Row, StatefulWidget, Table, TableState, Widget},
};

use crate::api::{Client, Model, ModelUpdates};

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
                        update.insert_to(&mut self.models.models);
                        sort_models(&mut self.models.models);
                    }
                    Err(e) => {
                        self.models.loading_state = LoadingState::Error(format!("{:?}", e));
                    }
                },

                else => break,
            }
        }
        Ok(())
    }

    pub fn render(&mut self, frame: &mut Frame) {
        let layout = Layout::vertical([Constraint::Length(1), Constraint::Fill(1)]);
        let [title_area, body_area] = frame.area().layout(&layout);

        let title = Line::from("llamm — Model Manager").centered().bold();
        frame.render_widget(title, title_area);
        frame.render_widget(&mut self.models, body_area);
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
                        let id = Cow::Owned(selected.id.clone());
                        let client = Arc::clone(client);
                        tokio::spawn(async move { client.load_model(id).await });
                    }
                }
                KeyCode::Delete | KeyCode::Char('u') => {
                    if let Some(selected) = self.models.selected_model() {
                        let id = Cow::Owned(selected.id.clone());
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
            Cow::Borrowed(value.status_label()),
            Cow::Borrowed(value.display_name()),
            Cow::Owned(value.human_size()),
        ])
    }
}

/// Sort models: loaded first, then sleeping, then loading, then alphabetical.
fn sort_models(models: &mut [Model]) {
    models.sort_by(|a, b| {
        status_order(a).cmp(&status_order(b)).then_with(|| {
            a.display_name()
                .to_lowercase()
                .cmp(&b.display_name().to_lowercase())
        })
    });
}

fn status_order(m: &Model) -> u8 {
    match m.status.value.as_str() {
        "loaded" => 0,
        "sleeping" => 1,
        "loading" | "downloading" => 2,
        _ => 3,
    }
}
