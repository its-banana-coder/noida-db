//! Rendering.

use super::api::Service;
use super::app::{Action, App, Mode};
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, Wrap};

pub fn human_bytes(b: u64) -> String {
    const U: [&str; 4] = ["B", "KB", "MB", "GB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i < 3 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 || v >= 10.0 {
        format!("{} {}", v.round() as u64, U[i])
    } else {
        format!("{v:.1} {}", U[i])
    }
}

pub fn service_color(name: &str) -> Color {
    match name {
        "postgres" => Color::Blue,
        "mysql" => Color::Yellow,
        "redis" => Color::Red,
        "kafka" => Color::Magenta,
        "elasticsearch" => Color::Green,
        "clickhouse" => Color::Cyan,
        "mongodb" => Color::LightGreen,
        _ => Color::White,
    }
}

/// UTC wall-clock HH:MM:SS (the contract only gives epoch milliseconds).
pub fn clock(ts_ms: u64) -> String {
    let s = ts_ms / 1000 % 86400;
    format!("{:02}:{:02}:{:02}", s / 3600, s / 60 % 60, s % 60)
}

fn fmt_uptime(ms: u64) -> String {
    let s = ms / 1000;
    if s >= 3600 {
        format!("{}h{:02}m", s / 3600, s / 60 % 60)
    } else if s >= 60 {
        format!("{}m{:02}s", s / 60, s % 60)
    } else {
        format!("{s}s")
    }
}

pub fn draw(f: &mut Frame, app: &App) {
    let n = app.services().len().max(1) as u16;
    let [header, table, footer, activity, msg, hints] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(n + 4),
        Constraint::Length(1),
        Constraint::Min(3),
        Constraint::Length(1),
        Constraint::Length(1),
    ])
    .areas(f.area());

    draw_header(f, app, header);
    draw_table(f, app, table);
    let connect = app.selected_service().map(|s| s.connect.as_str()).unwrap_or("");
    f.render_widget(
        Line::from(vec![
            Span::raw(" connect: "),
            Span::styled(connect, Style::new().add_modifier(Modifier::BOLD)),
        ]),
        footer,
    );
    draw_activity(f, app, activity);
    if let Some(b) = &app.busy {
        f.render_widget(Span::styled(format!(" {b}"), Style::new().fg(Color::Yellow)), msg);
    } else if let Some((m, err)) = &app.message {
        let c = if *err { Color::Red } else { Color::Green };
        f.render_widget(Span::styled(format!(" {m}"), Style::new().fg(c)), msg);
    }
    f.render_widget(
        Span::styled(
            " ↑↓ move  space on/off  c copy  r reset  f filter  s shutdown  q quit",
            Style::new().fg(Color::DarkGray),
        ),
        hints,
    );
    match &app.mode {
        Mode::Normal => {}
        Mode::NoServer => dialog(
            f,
            " No server ",
            &format!(
                "No noida-db server is running for {}.\n\nStart noida-db now? (y/n)",
                app.data_dir.display()
            ),
        ),
        Mode::Confirm(Action::Reset(n)) => dialog(
            f,
            " Reset ",
            &format!("Wipe all saved data of {n}?\n\nType y to confirm, anything else cancels."),
        ),
        Mode::Confirm(_) => dialog(
            f,
            " Shut down ",
            "Shut down the whole noida-db server?\n\nType y to confirm, anything else cancels.",
        ),
    }
}

fn draw_header(f: &mut Frame, app: &App, area: Rect) {
    let mut spans = vec![Span::styled(" noida-db", Style::new().add_modifier(Modifier::BOLD))];
    if let Some(st) = &app.status {
        spans.push(Span::raw(format!(" {}", st.version)));
        spans.push(Span::raw(format!(
            " · data {}",
            if st.data_dir.is_empty() {
                app.data_dir.display().to_string()
            } else {
                st.data_dir.clone()
            }
        )));
        if let Some(r) = st.rss_bytes {
            spans.push(Span::raw(format!(" · rss {}", human_bytes(r))));
        }
        spans.push(Span::raw(format!(" · up {}", fmt_uptime(st.uptime_ms))));
    } else {
        spans.push(Span::raw(format!(" · data {}", app.data_dir.display())));
    }
    if app.status.is_some() && !app.connected {
        spans.push(Span::styled(
            "  DISCONNECTED (retrying)",
            Style::new().fg(Color::Red).add_modifier(Modifier::BOLD),
        ));
    }
    f.render_widget(Line::from(spans), area);
}

fn state_cell(s: &Service) -> Cell<'static> {
    match s.state.as_str() {
        "running" => Cell::from("● on").style(Style::new().fg(Color::Green)),
        "stopped" => Cell::from("○ off").style(Style::new().fg(Color::DarkGray)),
        _ => Cell::from("– n/a").style(Style::new().fg(Color::DarkGray)),
    }
}

fn draw_table(f: &mut Frame, app: &App, area: Rect) {
    let rows: Vec<Row> = app
        .services()
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let sel = i == app.selected;
            let name = format!("{}{}", if sel { "▶" } else { " " }, s.name);
            let (connect, cstyle) = match (&s.error, s.state.as_str()) {
                (Some(e), _) => (format!("error: {e}"), Style::new().fg(Color::Red)),
                (None, "unavailable") => {
                    ("not in this build".to_string(), Style::new().fg(Color::DarkGray))
                }
                (None, _) => (s.connect.clone(), Style::new()),
            };
            let data = match (s.state.as_str(), s.data_bytes) {
                (_, Some(b)) => human_bytes(b),
                _ => "–".into(),
            };
            let port = if s.state == "unavailable" { "–".into() } else { s.port.to_string() };
            let row = Row::new(vec![
                Cell::from(name).style(Style::new().fg(service_color(&s.name))),
                state_cell(s),
                Cell::from(port),
                Cell::from(connect).style(cstyle),
                Cell::from(format!("{data:>8}")),
            ]);
            if sel {
                row.style(Style::new().add_modifier(Modifier::BOLD | Modifier::REVERSED))
            } else {
                row
            }
        })
        .collect();
    let table = Table::new(
        rows,
        [
            Constraint::Length(15),
            Constraint::Length(6),
            Constraint::Length(5),
            Constraint::Min(20),
            Constraint::Length(8),
        ],
    )
    .header(
        Row::new(["service", "state", "port", "connect with", "data"])
            .style(Style::new().add_modifier(Modifier::BOLD))
            .bottom_margin(1),
    )
    .block(Block::bordered());
    f.render_widget(table, area);
}

fn draw_activity(f: &mut Frame, app: &App, area: Rect) {
    let title = match &app.filter {
        Some(n) => format!(" activity: {n} (UTC) "),
        None => " activity (UTC) ".to_string(),
    };
    let block = Block::new().borders(Borders::TOP).title(title);
    let h = block.inner(area).height as usize;
    let mut lines: Vec<Line> = app
        .events
        .iter()
        .filter(|e| app.filter.as_ref().is_none_or(|n| *n == e.service))
        .map(|e| {
            Line::from(vec![
                Span::styled(format!(" {} ", clock(e.ts_ms)), Style::new().fg(Color::DarkGray)),
                Span::styled(
                    format!("{:<14}", e.service),
                    Style::new().fg(service_color(&e.service)),
                ),
                Span::raw(e.text.clone()),
            ])
        })
        .collect();
    let skip = lines.len().saturating_sub(h);
    lines.drain(..skip);
    f.render_widget(Paragraph::new(lines).block(block), area);
}

fn dialog(f: &mut Frame, title: &str, text: &str) {
    let a = f.area();
    let w = a.width.saturating_sub(4).clamp(10, 64);
    let h = 7.min(a.height);
    let r = Rect::new(
        a.x + (a.width.saturating_sub(w)) / 2,
        a.y + (a.height.saturating_sub(h)) / 2,
        w.min(a.width),
        h,
    );
    f.render_widget(Clear, r);
    f.render_widget(
        Paragraph::new(text.to_string())
            .alignment(Alignment::Center)
            .wrap(Wrap { trim: true })
            .block(
                Block::bordered()
                    .title(title.to_string())
                    .border_style(Style::new().fg(Color::Yellow)),
            ),
        r,
    );
}
