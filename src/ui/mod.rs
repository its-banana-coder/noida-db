//! `noida-db ui`: an interactive terminal panel, a client of the control API
//! (docs/control-api.md). Everything here is behind the `ui` feature.

mod api;
mod app;
#[cfg(test)]
mod fake;
mod http;
#[cfg(test)]
mod tests;
mod view;

use app::App;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::crossterm::{
    event::{self, Event},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn restore() {
    let _ = disable_raw_mode();
    let _ = execute!(io::stdout(), LeaveAlternateScreen);
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64(data: &[u8]) -> String {
    let mut out = String::new();
    for c in data.chunks(3) {
        let n = (c[0] as u32) << 16
            | (*c.get(1).unwrap_or(&0) as u32) << 8
            | *c.get(2).unwrap_or(&0) as u32;
        for i in 0..4 {
            if i <= c.len() {
                out.push(B64[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// The OSC 52 sequence that sets the system clipboard in most terminals.
fn osc52(text: &str) -> String {
    format!("\x1b]52;c;{}\x07", base64(text.as_bytes()))
}

pub fn run(data_dir: PathBuf) -> Result<(), String> {
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        restore();
        hook(info);
    }));
    enable_raw_mode().map_err(|e| e.to_string())?;
    execute!(io::stdout(), EnterAlternateScreen).map_err(|e| e.to_string())?;
    let res = event_loop(data_dir);
    restore();
    res.map_err(|e| e.to_string())
}

fn event_loop(data_dir: PathBuf) -> io::Result<()> {
    let mut term = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    let mut app = App::new(data_dir);
    app.connect_initial();
    let mut last_tick = Instant::now();
    loop {
        term.draw(|f| view::draw(f, &app))?;
        if app.quit {
            return Ok(());
        }
        // Resize events need nothing: the next draw re-lays-out.
        if event::poll(Duration::from_millis(100))?
            && let Event::Key(k) = event::read()?
            && let Some(action) = app.handle_key(k)
        {
            app.busy = Some(action.busy_text());
            term.draw(|f| view::draw(f, &app))?;
            app.perform(action);
            app.busy = None;
        }
        if let Some(text) = app.clipboard.take() {
            write!(term.backend_mut(), "{}", osc52(&text))?;
            term.backend_mut().flush()?;
        }
        if last_tick.elapsed() >= Duration::from_millis(500) {
            app.tick();
            last_tick = Instant::now();
        }
    }
}
