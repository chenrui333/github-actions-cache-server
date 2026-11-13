use bytes::{Buf, Bytes};
use opendal::{Operator, services::Ghac};

async fn verify(operator: &Operator, key: &str, expected: &[u8]) -> opendal::Result<()> {
    for _ in 0..2 {
        let mut body = operator.read(key).await?;
        assert_eq!(body.remaining(), expected.len(), "wrong restored size");
        let mut offset = 0;
        while body.has_remaining() {
            let chunk = body.chunk();
            assert!(
                chunk == &expected[offset..offset + chunk.len()],
                "wrong restored bytes"
            );
            offset += chunk.len();
            let size = chunk.len();
            body.advance(size);
        }
    }
    Ok(())
}

async fn roundtrip() -> opendal::Result<()> {
    let endpoint = std::env::var("FIXTURE_ENDPOINT").expect("FIXTURE_ENDPOINT is required");
    let address = endpoint.strip_prefix("http://")
        .and_then(|value| value.strip_suffix('/'))
        .and_then(|value| value.parse::<std::net::SocketAddr>().ok());
    assert!(address.is_some_and(|value| value.ip().is_loopback()), "fixture only permits a loopback endpoint");
    let token = std::env::var("FIXTURE_TOKEN").expect("FIXTURE_TOKEN is required");
    let prefix = std::env::var("FIXTURE_PREFIX").expect("FIXTURE_PREFIX is required");
    let operator = Operator::new(
        Ghac::default()
            .version("authenticated-completion-client-fixture")
            .endpoint(&endpoint)
            .runtime_token(&token),
    )?
    .finish();
    let chunk_size = 64 * 1024;
    let payload = Bytes::from(
        (0..chunk_size * 3)
            .map(|offset| b'A' + (offset / chunk_size) as u8)
            .collect::<Vec<_>>(),
    );
    let whole_key = format!("{prefix}-whole");
    operator.write(&whole_key, payload.slice(..1024)).await?;
    verify(&operator, &whole_key, &payload[..1024]).await?;

    let blocks_key = format!("{prefix}-blocks");
    let mut writer = operator
        .writer_with(&blocks_key)
        .chunk(chunk_size)
        .concurrent(3)
        .await?;
    for index in 0..3 {
        writer
            .write(payload.slice(index * chunk_size..(index + 1) * chunk_size))
            .await?;
    }
    writer.close().await?;
    verify(&operator, &blocks_key, &payload).await?;
    println!("{{\"whole\":true,\"multipart\":true,\"coldWarmExact\":true}}");
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(error) = roundtrip().await {
        // Preserve diagnostics but never print a bearer or signed URL query.
        let token = std::env::var("FIXTURE_TOKEN").unwrap_or_default();
        let message = error.to_string();
        let safe = if token.is_empty() {
            message
        } else {
            message.replace(&token, "[redacted]")
        };
        eprint!("OpenDAL roundtrip failed:");
        for word in safe.split_whitespace() {
            eprint!(" {}", word.split_once('?').map_or(word, |(path, _)| path));
        }
        eprintln!();
        std::process::exit(1);
    }
}
