use rynamodb::sync_actor::{ActorHandle, ShutdownState, SyncActor, SyncActorImpl};
use tokio::sync::oneshot;

struct Inner;

#[derive(Debug)]
enum Message {
    Foo { tx: oneshot::Sender<i32> },
    Shutdown,
}

impl SyncActorImpl for Inner {
    type Message = Message;

    fn handle_message(&mut self, message: Self::Message) -> rynamodb::sync_actor::ShutdownState {
        match message {
            Message::Foo { tx: sender } => {
                println!("Got request for foo");
                let _ = sender.send(10);
                ShutdownState::Continue
            }
            Message::Shutdown => {
                println!("shutting down actor");
                ShutdownState::Break
            }
        }
    }
}

struct CustomHandle(ActorHandle<Inner>);

impl CustomHandle {
    async fn get_value(&self) -> i32 {
        let (tx, rx) = oneshot::channel();
        let msg = Message::Foo { tx };
        self.0.send(msg).await;
        rx.await.unwrap()
    }

    async fn shutdown(self) {
        let msg = Message::Shutdown;
        self.0.send(msg).await;
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let actor = CustomHandle(SyncActor::handle(Inner));

    let value = actor.get_value().await;
    println!("{}", value);
    actor.shutdown().await;

    return;

    let app = rynamodb::router();
    let port = 3050;
    tracing::info!(%port, "running server");
    rynamodb::run_server(app, port).await.unwrap();
}
