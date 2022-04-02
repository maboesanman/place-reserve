#![feature(async_closure)]

use futures::{FutureExt, StreamExt};
use image::{ImageOutputFormat, Rgba, RgbaImage};
use serde::Deserialize;
use std::{
    collections::HashSet,
    io::Cursor,
    sync::{Mutex, RwLock},
    time::Duration,
};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use warp::{
    http::HeaderValue,
    hyper::{header::CONTENT_TYPE, Response},
    Filter,
};

#[derive(Debug)]
struct Reservation {
    x: u32,
    y: u32,
}

#[derive(Debug)]
struct Expiration {
    x: u32,
    y: u32,
}

#[derive(Debug)]
struct State {
    pub image_buffer: RgbaImage,
    pub active_reservations: HashSet<(u32, u32)>,
}

#[derive(Deserialize, Debug)]
struct Query {
    x: u32,
    y: u32,
}

const X_DIM: u32 = 20;
const Y_DIM: u32 = 20;
const TIMEOUT_DURATION: Duration = Duration::new(60 * 5, 0);

#[tokio::main]
async fn main() {
    let (res_sender, res_stream) = mpsc::unbounded_channel::<Reservation>();
    let (exp_sender, exp_stream) = mpsc::unbounded_channel::<Expiration>();

    let sender = Box::leak(Box::new(res_sender));
    // let active_reservations: Mutex<BTreeMap<Instant, (u8, u8)>> = todo!();
    // let image_buffer: Image

    let image_buffer = RgbaImage::new(X_DIM, Y_DIM);
    let image: &'static RwLock<Vec<u8>> = {
        let mut cursor = Cursor::new(Vec::new());
        image_buffer
            .write_to(&mut cursor, ImageOutputFormat::Png)
            .unwrap();
        let vec = cursor.into_inner();

        Box::leak(Box::new(RwLock::new(vec)))
    };

    let state = State {
        image_buffer,
        active_reservations: HashSet::new(),
    };
    let state = Box::leak(Box::new(Mutex::new(state)));

    let add_fut = add_reservations(res_stream, exp_sender, image, state);
    let exp_fut = expire_reservations(exp_stream, state, image);
    let web_fut = serve(sender, image);

    tokio::join!(add_fut, exp_fut, web_fut);
}

async fn add_reservations(
    res_stream: UnboundedReceiver<Reservation>,
    exp_sender: UnboundedSender<Expiration>,
    image: &'static RwLock<Vec<u8>>,
    state: &'static Mutex<State>,
) -> ! {
    let res_stream = UnboundedReceiverStream::new(res_stream);
    let res_stream_chunks = res_stream.ready_chunks(1000);
    let exp_sender: &'static UnboundedSender<_> = Box::leak(Box::new(exp_sender));

    res_stream_chunks
        .for_each(|chunk| {
            let mut state_lock = state.lock().unwrap();
            for res in chunk.into_iter() {
                let Reservation { x, y } = res;
                state_lock.active_reservations.insert((x, y));
                state_lock
                    .image_buffer
                    .put_pixel(x, y, Rgba([0, 0, 0, 255]));

                tokio::spawn(tokio::time::sleep(TIMEOUT_DURATION).map(move |()| {
                    exp_sender.send(Expiration { x, y }).unwrap();
                }));
            }
            let vec = write_buf_to_vec(&state_lock.image_buffer);
            drop(state_lock);

            let mut image_lock = image.write().unwrap();
            *image_lock = vec;
            drop(image_lock);

            futures::future::ready(())
        })
        .await;

    unreachable!()
}

async fn expire_reservations(
    exp_stream: UnboundedReceiver<Expiration>,
    state: &'static Mutex<State>,
    image: &'static RwLock<Vec<u8>>,
) -> ! {
    let exp_stream = UnboundedReceiverStream::new(exp_stream);
    let exp_stream_chunks = exp_stream.ready_chunks(1000);

    exp_stream_chunks
        .for_each(|chunk| {
            let mut state_lock = state.lock().unwrap();
            for exp in chunk.into_iter() {
                let Expiration { x, y } = exp;
                state_lock.active_reservations.remove(&(x, y));
                state_lock.image_buffer.put_pixel(x, y, Rgba([0, 0, 0, 0]));
            }
            let vec = write_buf_to_vec(&state_lock.image_buffer);
            drop(state_lock);

            let mut image_lock = image.write().unwrap();
            *image_lock = vec;
            drop(image_lock);

            futures::future::ready(())
        })
        .await;

    unreachable!()
}

async fn serve(
    sender: &'static UnboundedSender<Reservation>,
    image: &'static RwLock<Vec<u8>>,
) -> ! {
    let image = warp::path!("image").and(warp::get()).map(|| {
        let lock = image.read().unwrap();
        let data = lock.clone();
        drop(lock);
        let mut res = Response::new(data);
        res.headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("image/png"));
        res
    });

    let reserve = warp::path!("reserve")
        .and(warp::get())
        .and(warp::query::<Query>())
        .map(|Query { x, y }| {
            reserve(x, y, sender);
            Response::new("")
        });

    let routes = warp::any().and(image.or(reserve));

    warp::serve(routes).run(([127, 0, 0, 1], 8000)).await;

    unreachable!()
}

fn reserve(x: u32, y: u32, sender: &UnboundedSender<Reservation>) {
    if x >= X_DIM || y >= Y_DIM {
        return;
    }

    sender.send(Reservation { x, y }).unwrap();
}

fn write_buf_to_vec(buf: &RgbaImage) -> Vec<u8> {
    let mut cursor = Cursor::new(Vec::new());
    buf.write_to(&mut cursor, ImageOutputFormat::Png).unwrap();
    cursor.into_inner()
}
