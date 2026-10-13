use super::app::{Action, App, Mode};
use super::fake::Fake;
use super::view::{clock, draw, human_bytes};
use super::{base64, osc52};
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

fn key(c: KeyCode) -> KeyEvent {
    KeyEvent::new(c, KeyModifiers::NONE)
}

fn attached(fake: &Fake) -> App {
    let dir = std::env::temp_dir().join(format!("noida-ui-test-{}", fake.port));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("noida.control.json"),
        format!(
            r#"{{"pid":1,"port":{},"token":"{}","version":"0.2.3","started_ms":0}}"#,
            fake.port, fake.token
        ),
    )
    .unwrap();
    let mut app = App::new(dir);
    app.connect_initial();
    app
}

fn screen(app: &App, w: u16, h: u16) -> String {
    let mut t = Terminal::new(TestBackend::new(w, h)).unwrap();
    t.draw(|f| draw(f, app)).unwrap();
    let buf = t.backend().buffer();
    (0..h)
        .map(|y| (0..w).map(|x| buf[(x, y)].symbol().to_string()).collect::<String>())
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn helpers() {
    assert_eq!(human_bytes(0), "0 B");
    assert_eq!(human_bytes(6 * 1024 * 1024), "6.0 MB");
    assert_eq!(human_bytes(50 * 1024 * 1024), "50 MB");
    assert_eq!(clock(43_263_000), "12:01:03");
    assert_eq!(base64(b"Man"), "TWFu");
    assert_eq!(base64(b"Ma"), "TWE=");
    assert_eq!(base64(b"M"), "TQ==");
    assert_eq!(osc52("hi"), "\x1b]52;c;aGk=\x07");
}

#[test]
fn client_parses_status_and_activity_since() {
    let fake = Fake::start();
    let c = fake.client();
    let st = c.status().unwrap();
    assert_eq!(st.services.len(), 3);
    assert_eq!(st.services[0].data_bytes, Some(6291456));
    assert_eq!(st.rss_bytes, Some(7340032));
    assert_eq!(c.activity(0).unwrap().events.len(), 3);
    let a = c.activity(2).unwrap();
    assert_eq!((a.events.len(), a.next), (1, 3));
    let bad = super::api::Client { port: fake.port, token: "wrong".into() };
    assert!(matches!(bad.status(), Err(super::http::ApiError::Http(401, _))));
}

#[test]
fn renders_table_activity_and_footer() {
    let fake = Fake::start();
    let app = attached(&fake);
    let s = screen(&app, 100, 24);
    assert!(s.contains("noida-db 0.2.3"), "{s}");
    assert!(s.contains("rss 7.0 MB"), "{s}");
    assert!(s.contains("▶postgres"), "{s}");
    assert!(s.contains("● on"), "{s}");
    assert!(s.contains("○ off"), "{s}");
    assert!(s.contains("not in this build"), "{s}");
    assert!(s.contains("6.0 MB"), "{s}");
    assert!(s.contains("connect: postgres://postgres@127.0.0.1:5432/postgres"), "{s}");
    assert!(s.contains("12:01:05 redis"), "{s}");
    assert!(s.contains("CREATE TABLE users"), "{s}");
}

#[test]
fn renders_in_tiny_terminal_without_panic() {
    let fake = Fake::start();
    let app = attached(&fake);
    for (w, h) in [(10, 3), (40, 8), (1, 1), (200, 60)] {
        screen(&app, w, h);
    }
}

#[test]
fn toggle_sends_the_right_post() {
    let fake = Fake::start();
    let mut app = attached(&fake);
    let a = app.handle_key(key(KeyCode::Char(' '))).unwrap();
    assert_eq!(a, Action::Stop("postgres".into()));
    app.perform(a);
    assert!(fake.requests().contains(&"POST /v1/services/postgres/stop".to_string()));
    assert_eq!(app.services()[0].state, "stopped");
    app.handle_key(key(KeyCode::Down));
    let a = app.handle_key(key(KeyCode::Enter)).unwrap();
    assert_eq!(a, Action::Start("redis".into()));
    app.perform(a);
    assert_eq!(app.services()[1].state, "running");
    // unavailable service: no action, an error message
    app.handle_key(key(KeyCode::Down));
    assert!(app.handle_key(key(KeyCode::Char(' '))).is_none());
    assert!(app.message.as_ref().unwrap().1);
}

#[test]
fn reset_and_shutdown_need_confirmation() {
    let fake = Fake::start();
    let mut app = attached(&fake);
    assert!(app.handle_key(key(KeyCode::Char('r'))).is_none());
    assert_eq!(app.mode, Mode::Confirm(Action::Reset("postgres".into())));
    assert!(screen(&app, 100, 24).contains("Type y to confirm"));
    assert!(app.handle_key(key(KeyCode::Char('x'))).is_none());
    assert_eq!(app.mode, Mode::Normal);
    assert!(!fake.requests().iter().any(|r| r.contains("reset")));
    app.handle_key(key(KeyCode::Char('r')));
    let a = app.handle_key(key(KeyCode::Char('y'))).unwrap();
    app.perform(a);
    assert!(fake.requests().contains(&"POST /v1/services/postgres/reset".to_string()));
    app.handle_key(key(KeyCode::Char('s')));
    let a = app.handle_key(key(KeyCode::Char('y'))).unwrap();
    assert_eq!(a, Action::Shutdown);
    app.perform(a);
    assert!(fake.state.lock().unwrap().shutdown);
}

#[test]
fn quit_does_not_touch_server_and_copy_sets_clipboard() {
    let fake = Fake::start();
    let mut app = attached(&fake);
    app.handle_key(key(KeyCode::Char('c')));
    assert_eq!(app.clipboard.as_deref(), Some("postgres://postgres@127.0.0.1:5432/postgres"));
    app.handle_key(key(KeyCode::Char('q')));
    assert!(app.quit);
    assert!(!fake.requests().iter().any(|r| r.contains("shutdown")));
}

#[test]
fn activity_polls_with_since_and_filters() {
    let fake = Fake::start();
    let mut app = attached(&fake);
    assert_eq!(app.events.len(), 3);
    app.tick();
    assert_eq!(app.events.len(), 3, "no duplicates");
    fake.state.lock().unwrap().events.push((4, "redis".into(), "GET a".into()));
    app.tick();
    assert_eq!(app.events.len(), 4);
    assert!(fake.requests().iter().any(|r| r.contains("since=3")));
    app.handle_key(key(KeyCode::Char('f')));
    let s = screen(&app, 100, 24);
    assert!(s.contains("activity: postgres"), "{s}");
    assert!(s.contains("SELECT 1") && !s.contains("GET a"), "{s}");
    app.handle_key(key(KeyCode::Char('f')));
    assert!(app.filter.is_none());
}

#[test]
fn disconnect_is_shown_and_recovers() {
    let fake = Fake::start();
    let mut app = attached(&fake);
    // Point at a dead port.
    app.client = Some(super::api::Client { port: 1, token: "x".into() });
    std::fs::remove_file(app.data_dir.join("noida.control.json")).unwrap();
    app.tick();
    assert!(!app.connected);
    assert!(screen(&app, 100, 24).contains("DISCONNECTED"));
    std::fs::write(
        app.data_dir.join("noida.control.json"),
        format!(r#"{{"port":{},"token":"{}"}}"#, fake.port, fake.token),
    )
    .unwrap();
    app.tick();
    assert!(app.connected);
}

#[test]
fn no_server_offers_to_start() {
    let dir = std::env::temp_dir().join("noida-ui-test-empty");
    let _ = std::fs::create_dir_all(&dir);
    let _ = std::fs::remove_file(dir.join("noida.control.json"));
    let mut app = App::new(dir);
    app.connect_initial();
    assert_eq!(app.mode, Mode::NoServer);
    assert!(screen(&app, 100, 24).contains("Start noida-db now? (y/n)"));
    assert_eq!(app.handle_key(key(KeyCode::Char('y'))), Some(Action::Launch));
    app.handle_key(key(KeyCode::Char('n')));
    assert!(app.quit);
}
