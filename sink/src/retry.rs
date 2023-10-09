use std::{pin::Pin, time::Duration};

use futures03::Future;

use tokio::time::sleep;

// Define a type for functions that produce a Future.
type AsyncFn<T, E> = Box<dyn Fn() -> Pin<Box<dyn Future<Output = Result<T, E>> + Send>> + Send>;

pub struct Retry<T, E> {
    func: AsyncFn<T, E>,
    max_retries: usize,
}

impl<T, E> Retry<T, E>
where
    E: std::fmt::Debug, // for println!() in retry method
{
    pub fn new(func: AsyncFn<T, E>, max_retries: usize) -> Self {
        Self { func, max_retries }
    }

    pub async fn retry(&self) -> Result<T, E> {
        let mut retries = 0;
        loop {
            match (self.func)().await {
                Ok(val) => return Ok(val),
                Err(err) => {
                    if retries >= self.max_retries {
                        return Err(err);
                    }
                    retries += 1;
                    println!("Error: {:?}. Retrying... Attempt #{}", err, retries + 1);
                    // make the sleep duration random between 0.1 and 0.3 seconds
                    let sleep_duration = Duration::from_millis(rand::random::<u64>() % 200 + 100);
                    sleep(sleep_duration).await; // wait for a second before retrying
                }
            }
        }
    }
}
