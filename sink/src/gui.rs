use anyhow::Error;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, prelude::*, widgets::*, Terminal};
use std::{
    io::{self, Stdout},
    thread,
};

pub struct StatefulList<T> {
    state: ListState,
    pub items: Vec<T>,
}

impl<T> StatefulList<T> {
    pub fn new() -> StatefulList<T> {
        StatefulList {
            state: ListState::default(),
            items: Vec::new(),
        }
    }
    pub fn push(&mut self, item: T) {
        self.items.push(item);
    }

    pub fn with_items(items: Vec<T>) -> StatefulList<T> {
        StatefulList {
            state: ListState::default(),
            items,
        }
    }

    pub fn next(&mut self) {
        let i = match self.state.selected() {
            Some(i) => {
                if i >= self.items.len() - 1 {
                    0
                } else {
                    i + 1
                }
            }
            None => 0,
        };
        self.state.select(Some(i));
    }

    pub fn previous(&mut self) {
        let i = match self.state.selected() {
            Some(i) => {
                if i == 0 {
                    self.items.len() - 1
                } else {
                    i - 1
                }
            }
            None => 0,
        };
        self.state.select(Some(i));
    }

    pub fn unselect(&mut self) {
        self.state.select(None);
    }
}

pub struct GuiData {
    pub start_block: u64,
    pub stop_block: u64,
    pub block_number: u64,
    pub information_in_block: bool,
    pub tasks: StatefulList<String>,
}

pub fn ui(
    f: &mut Frame<CrosstermBackend<Stdout>>,
    app: &GuiData,
    render_count: i32,
) -> Result<(), Error> {
    let GuiData {
        start_block,
        stop_block,
        block_number,
        information_in_block,
        tasks,
    } = &app;

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints(
            [
                Constraint::Percentage(10),
                Constraint::Percentage(10),
                Constraint::Percentage(10),
                Constraint::Percentage(70),
            ]
            .as_ref(),
        )
        .split(f.size());

    // Draw the header that shows what block we are on
    let title = match *information_in_block {
        true => format!("Block {} (PROCESSING TRIPLE DATA)", block_number),
        false => format!("Block {} (no information)", block_number),
    };

    let bg = match *information_in_block {
        true => Color::Green,
        false => Color::Red,
    };

    let size = chunks[0];
    let block = Block::default().title(title).bg(bg).borders(Borders::ALL);
    f.render_widget(block, size);

    // Draw a title that shows how many renders we have done
    let render_title = format!("Render count: {}", render_count);
    let size = chunks[1];
    let block = Block::default()
        .title(render_title)
        .bg(Color::Blue)
        .borders(Borders::ALL);
    f.render_widget(block, size);

    // Show the start block and stop block
    let size = chunks[2];

    let percent = match block_number {
        0 => 0,
        _ => (block_number / stop_block) * 100,
    };

    let gauge = Gauge::default()
        .block(
            Block::default()
                .title("Indexing Progress")
                .borders(Borders::ALL),
        )
        .gauge_style(Style::default().fg(Color::Yellow))
        .percent(percent as u16);
    f.render_widget(gauge, size);

    // // The event list doesn't have any state and only displays the current state of the list.
    let task_list = app
        .tasks
        .items
        .iter()
        .rev()
        .map(|task| {
            // Colorcode the level depending on its type
            let s = match task {
                _ => Style::default().fg(Color::White),
            };
            // Add a example datetime and apply proper spacing between them
            let log = Line::from(vec![task.into()]);

            // Here several things happen:
            // 1. Add a `---` spacing line above the final list entry
            // 2. Add the Level + datetime
            // 3. Add a spacer line
            // 4. Add the actual event
            ListItem::new(vec![Line::from("-".repeat(chunks[1].width as usize)), log])
        })
        .collect::<Vec<_>>();

    // Show the tasks
    let size = chunks[3];
    let task_widget =
        List::new(task_list).block(Block::default().borders(Borders::ALL).title("Tasks"));
    f.render_widget(task_widget, size);
    //f.render_stateful_widget(task_widget, size, &mut app.tasks.state);
    Ok(())
}
