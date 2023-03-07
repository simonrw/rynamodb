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
    I: SyncActorImpl,
{
    fn new(inner: I) -> (Self, mpsc::Sender<I::Message>) {
        let (tx, rx) = mpsc::channel(10);
        let me = Self { rx, inner };
        (me, tx)
    }

    fn run(&mut self) {
        loop {
            if let Some(msg) = self.rx.blocking_recv() {
                if let ShutdownState::Break = self.inner.handle_message(msg) {
                    break;
                }
            }
        }
    }
}
