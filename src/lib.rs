use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt};
use std::{
    collections::HashMap,
    result::Result,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time;

type City = String;
type Temperature = u64;

#[async_trait]
pub trait Api: Send + Sync + 'static {
    async fn fetch(&self) -> Result<HashMap<City, Temperature>, String>;
    async fn subscribe(&self) -> BoxStream<Result<(City, Temperature), String>>;
}

pub struct StreamCache {
    results: Arc<Mutex<HashMap<String, u64>>>,
}

impl StreamCache {
    pub fn new(api: impl Api) -> Self {
        let instance = Self {
            results: Arc::new(Mutex::new(HashMap::new())),
        };
        instance.update_in_background(api);
        instance
    }

    pub fn get(&self, key: &str) -> Option<u64> {
        let results = self.results.lock().expect("poisoned");
        results.get(key).copied()
    }

    pub fn update_in_background(&self, api: impl Api) {
        let api_arc = Arc::new(api);

        self.fetch_in_background(&api_arc);
        self.subscribe_in_background(&api_arc);
    }

    fn fetch_in_background(&self, api_arc: &Arc<impl Api>) {
        let results = Arc::clone(&self.results);
        let api = Arc::clone(api_arc);

        tokio::spawn(async move {
            match api.fetch().await {
                Ok(data) => {
                    for (city, temperature) in data {
                        let mut results = results.lock().expect("poisoned");
                        results.entry(city).or_insert(temperature); // prioritize 'subscribe'
                    }
                }
                Err(err) => {
                    eprintln!("Error fetching api: {err}");
                }
            }
        });
    }

    fn subscribe_in_background(&self, api_arc: &Arc<impl Api>) {
        let results = Arc::clone(&self.results);
        let api = Arc::clone(api_arc);

        tokio::spawn(async move {
            loop {
                let mut stream = api.subscribe().await;
                while let Some(data) = stream.next().await {
                    match data {
                        Ok((city, temperature)) => {
                            let mut results = results.lock().expect("poisoned");
                            results.insert(city, temperature);
                        }
                        Err(err) => {
                            eprintln!("Error subscribing to api: {err}");
                        }
                    }
                }

                time::sleep(Duration::from_millis(1)).await; // sleep to throttle the api calls - CHANGE ME
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use tokio::sync::Notify;
    use tokio::time;

    use futures::{future, stream::select, FutureExt, StreamExt};
    use maplit::hashmap;

    use super::*;

    #[derive(Default)]
    struct TestApi {
        signal: Arc<Notify>,
    }

    #[async_trait]
    impl Api for TestApi {
        async fn fetch(&self) -> Result<HashMap<City, Temperature>, String> {
            // fetch is slow an may get delayed until after we receive the first updates
            self.signal.notified().await;
            Ok(hashmap! {
                "Berlin".to_string() => 29,
                "Paris".to_string() => 31,
            })
        }
        async fn subscribe(&self) -> BoxStream<Result<(City, Temperature), String>> {
            let results = vec![
                Ok(("London".to_string(), 27)),
                Ok(("Paris".to_string(), 32)),
            ];
            select(
                futures::stream::iter(results),
                async {
                    self.signal.notify_one();
                    future::pending().await
                }
                .into_stream(),
            )
            .boxed()
        }
    }
    #[tokio::test]
    async fn works() {
        let cache = StreamCache::new(TestApi::default());

        // Allow cache to update
        time::sleep(Duration::from_millis(100)).await;

        assert_eq!(cache.get("Berlin"), Some(29));
        assert_eq!(cache.get("London"), Some(27));
        assert_eq!(cache.get("Paris"), Some(32));
    }

    #[derive(Default)]
    struct TestApiError {}

    #[async_trait]
    impl Api for TestApiError {
        async fn fetch(&self) -> Result<HashMap<City, Temperature>, String> {
            Err("fetch error".to_string())
        }
        async fn subscribe(&self) -> BoxStream<Result<(City, Temperature), String>> {
            let errors = vec![Err("fetch error".to_string())];
            futures::stream::iter(errors).boxed()
        }
    }

    #[tokio::test]
    async fn errors() {
        let cache = StreamCache::new(TestApiError::default());

        // Allow cache to update
        time::sleep(Duration::from_millis(100)).await;

        assert_eq!(cache.get("Berlin"), None);
    }

    #[derive(Default)]
    struct TestApiSubscribe {}

    #[async_trait]
    impl Api for TestApiSubscribe {
        async fn fetch(&self) -> Result<HashMap<City, Temperature>, String> {
            Ok(hashmap! {"Berlin".to_string() => 29})
        }
        async fn subscribe(&self) -> BoxStream<Result<(City, Temperature), String>> {
            let first_result = vec![Ok(("London".to_string(), 27))];
            let second_result = vec![Ok(("London".to_string(), 28))];
            select(
                futures::stream::iter(first_result),
                async {
                    time::sleep(Duration::from_millis(10)).await;
                    futures::stream::iter(second_result)
                }
                .await,
            )
            .boxed()
        }
    }

    #[tokio::test]
    async fn subscribes() {
        let cache = StreamCache::new(TestApiSubscribe::default());

        // Allow cache to update
        time::sleep(Duration::from_millis(100)).await;

        assert_eq!(cache.get("Berlin"), Some(29));
        assert_eq!(cache.get("London"), Some(28));
    }
}
