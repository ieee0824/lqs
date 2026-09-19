use lqs::{ServerConfig, serve};

#[tokio::main]
async fn main() {
    let config = match ServerConfig::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("invalid server configuration: {error}");
            std::process::exit(2);
        }
    };

    println!(
        "LQS listening on {} (database: {})",
        config.bind_addr,
        config.database_path.display()
    );
    if let Err(error) = serve(config).await {
        eprintln!("LQS server failed: {error}");
        std::process::exit(1);
    }
}
