use tokio::sync::mpsc;

pub enum ShutdownState {
    Continue,
    Break,
}

impl Default for ShutdownState {
    fn default() -> Self {
        Self::Continue
    }
}

pub trait SyncActorImpl {
    type Message;

    fn handle_message(&mut self, message: Self::Message) -> ShutdownState;
}

pub struct SyncActor<I>
where
    I: SyncActorImpl,
{
    rx: mpsc::Receiver<I::Message>,
    inner: I,
}

impl<I> SyncActor<I>
where
    I: SyncActorImpl + Send + 'static,
    I::Message: Send,
{
    pub fn handle(inner: I) -> ActorHandle<I> {
        ActorHandle::new(inner)
    }
}

#[derive(Clone)]
pub struct ActorHandle<I>
where
    I: SyncActorImpl,
{
    sender: mpsc::Sender<I::Message>,
}

impl<I> ActorHandle<I>
where
    I: SyncActorImpl + Send + 'static,
    I::Message: Send,
{
    pub fn new(inner: I) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let actor = SyncActor {
            rx: receiver,
            inner,
        };
        tokio::task::spawn_blocking(move || run_my_actor(actor));
        Self { sender }
    }

    pub async fn send(&self, msg: I::Message) {
        let _ = self.sender.send(msg).await;
    }
}

fn run_my_actor<I>(mut actor: SyncActor<I>)
where
    I: SyncActorImpl,
{
    loop {
        if let Some(msg) = actor.rx.blocking_recv() {
            if let ShutdownState::Break = actor.inner.handle_message(msg) {
                break;
            }
        }
    }
}
