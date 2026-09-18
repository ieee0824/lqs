use lqs::{Lqs, QueueOptions, QueueType, SendRequest};

fn main() {
    let mut lqs = Lqs::new();
    lqs.create_queue(
        "orders.fifo",
        QueueType::Fifo,
        QueueOptions {
            content_based_deduplication: true,
            ..QueueOptions::default()
        },
    )
    .expect("valid demo queue");

    lqs.send(
        "orders.fifo",
        SendRequest::fifo("order-A: created", "order-A"),
        0,
    )
    .unwrap();
    lqs.send(
        "orders.fifo",
        SendRequest::fifo("order-A: paid", "order-A"),
        1,
    )
    .unwrap();
    lqs.send(
        "orders.fifo",
        SendRequest::fifo("order-B: created", "order-B"),
        2,
    )
    .unwrap();

    let batch = lqs.receive("orders.fifo", 10, 3).unwrap();
    for message in &batch {
        println!(
            "{} [{}]",
            message.body,
            message.message_group_id.as_deref().unwrap_or("standard")
        );
    }
    // Deleting order-A's first event makes its next event eligible for delivery.
    lqs.delete("orders.fifo", &batch[0].receipt_handle).unwrap();
    println!(
        "next: {}",
        lqs.receive("orders.fifo", 1, 4).unwrap()[0].body
    );
}
