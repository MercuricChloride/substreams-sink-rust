use anyhow::Error;
use crossterm::terminal::{disable_raw_mode, LeaveAlternateScreen};
use std::io::{self, Stdout};
use std::process::exit;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crossterm::{
    execute,
    terminal::{enable_raw_mode, EnterAlternateScreen},
};
use ratatui::{prelude::CrosstermBackend, Terminal};

use crate::gui::{ui, GuiData};

/// Sets up the terminal to be used as a TUI
fn setup_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>, Error> {
    let mut stdout = io::stdout();
    enable_raw_mode()?;
    execute!(stdout, EnterAlternateScreen)?;
    Ok(Terminal::new(CrosstermBackend::new(stdout))?)
}

/// Runs the TUI
async fn run(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    gui_data_lock: Arc<RwLock<GuiData>>,
) -> Result<(), Error> {
    loop {
        terminal.clear()?;

        let gui_data = gui_data_lock.read().await;

        if gui_data.should_quit {
            break;
        }

        terminal.draw(|frame| {
            ui(frame, &gui_data);
        });
        drop(gui_data);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

/// Restores the terminal to its original state
fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<Stdout>>) -> Result<(), Error> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen,)?;
    Ok(terminal.show_cursor()?)
}

/// A function that returns a join handler for the gui thread
/// If the use_gui flag is set to false, this function will return a dummy thread
pub fn tui_handle(lock: Arc<RwLock<GuiData>>, use_gui: bool) -> JoinHandle<()> {
    if use_gui {
        tokio::task::spawn(async move {
            let mut terminal = setup_terminal().unwrap();
            run(&mut terminal, lock).await;
            restore_terminal(&mut terminal);
            exit(0)
        })
    } else {
        tokio::task::spawn(async move {})
    }
}
