#![feature(async_closure)]

use futures::{FutureExt, StreamExt};
use image::{ImageOutputFormat, Rgba, RgbaImage};
use serde::Deserialize;
use std::{
    collections::HashSet,
    io::Cursor,
    sync::{Mutex, RwLock, atomic::{AtomicUsize, Ordering}},
    time::Duration,
};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use warp::{
    http::HeaderValue,
    hyper::{header::CONTENT_TYPE, Response},
    Filter,
};

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
struct Reservation {
    x: u32,
    y: u32,
    index: usize,
}

#[derive(Debug)]
struct State {
    pub cdn_buffer: RgbaImage,
    pub image_buffer: RgbaImage,
    pub active_reservations: HashSet<Reservation>,
}

#[derive(Deserialize, Debug)]
struct Query {
    x: u32,
    y: u32,
}

const X_DIM: u32 = 2000;
const Y_DIM: u32 = 1000;
const TIMEOUT_DURATION: Duration = Duration::new(60 * 5, 0);
const CDN_URL: &'static str = "http://cdn.mirai.gg/tmp/dotted-place-template.png";
// const CDN_URL: &'static str = "https://cdn.discordapp.com/attachments/297619185926668290/959898636974702682/dotted_505png_v3.png";
const CDN_REFRESH: Duration = Duration::new(15, 0);

#[tokio::main]
async fn main() {
    let (res_sender, res_stream) = mpsc::unbounded_channel::<Reservation>();
    let (exp_sender, exp_stream) = mpsc::unbounded_channel::<Reservation>();

    let sender = Box::leak(Box::new(res_sender));
    // let active_reservations: Mutex<BTreeMap<Instant, (u8, u8)>> = todo!();
    // let image_buffer: Image

    let image_buffer = RgbaImage::new(X_DIM * 3, Y_DIM * 3);
    let image: &'static RwLock<Vec<u8>> = {
        
        let mut cursor = Cursor::new(Vec::new());
        image_buffer
            .write_to(&mut cursor, ImageOutputFormat::Png)
            .unwrap();
        let vec = cursor.into_inner();

        Box::leak(Box::new(RwLock::new(vec)))
    };

    let cdn_id: &'static AtomicUsize = Box::leak(Box::new(0.into()));
    let state = State {
        cdn_buffer: RgbaImage::new(X_DIM * 3, Y_DIM * 3),
        image_buffer: RgbaImage::new(X_DIM * 3, Y_DIM * 3),
        active_reservations: HashSet::new(),
    };
    let state = Box::leak(Box::new(Mutex::new(state)));

    let add_fut = add_reservations(res_stream, exp_sender, image, state);
    let exp_fut = expire_reservations(exp_stream, state, image, cdn_id);
    let web_fut = serve(sender, image, cdn_id);
    let cdn_fut = maintain_cdn_buf(state, cdn_id, image);

    tokio::join!(add_fut, exp_fut, web_fut, cdn_fut);
}

async fn add_reservations(
    res_stream: UnboundedReceiver<Reservation>,
    exp_sender: UnboundedSender<Reservation>,
    image: &'static RwLock<Vec<u8>>,
    state: &'static Mutex<State>
) -> ! {
    let res_stream = UnboundedReceiverStream::new(res_stream);
    let res_stream_chunks = res_stream.ready_chunks(1000);
    let exp_sender: &'static UnboundedSender<_> = Box::leak(Box::new(exp_sender));

    res_stream_chunks
        .for_each(|chunk| {
            let mut state_lock = state.lock().unwrap();
            for res in chunk.into_iter() {
                let Reservation { x, y, index: _ } = res;
                if 3 * x >= state_lock.image_buffer.width() || 3 * y >= state_lock.image_buffer.height() {
                    continue
                }
                let color = state_lock.cdn_buffer.get_pixel(3 * x + 1, 3 * y + 1).clone();
                if color.0[3] != 0 {
                    state_lock.active_reservations.insert(res.clone());

                    state_lock.image_buffer.put_pixel(3 * x, 3 * y + 1, color);
                    state_lock.image_buffer.put_pixel(3 * x + 1, 3 * y, color);
                    state_lock.image_buffer.put_pixel(3 * x + 2, 3 * y + 1, color);
                    state_lock.image_buffer.put_pixel(3 * x + 1, 3 * y + 2, color);

                    tokio::spawn(tokio::time::sleep(TIMEOUT_DURATION).map(move |()| {
                        exp_sender.send(res).unwrap();
                    }));
                }
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
    exp_stream: UnboundedReceiver<Reservation>,
    state: &'static Mutex<State>,
    image: &'static RwLock<Vec<u8>>,
    cdn_id: &'static AtomicUsize
) -> ! {
    let exp_stream = UnboundedReceiverStream::new(exp_stream);
    let exp_stream_chunks = exp_stream.ready_chunks(1000);

    exp_stream_chunks
        .for_each(|chunk| {
            let mut state_lock = state.lock().unwrap();
            let cdn_index = cdn_id.load(Ordering::SeqCst);
            for exp in chunk.into_iter() {
                state_lock.active_reservations.remove(&exp);
                let Reservation { x, y, index } = exp;
                if cdn_index == index {
                    let color = Rgba([0, 0, 0, 0]);
                    state_lock.image_buffer.put_pixel(3 * x, 3 * y + 1, color);
                    state_lock.image_buffer.put_pixel(3 * x + 1, 3 * y, color);
                    state_lock.image_buffer.put_pixel(3 * x + 2, 3 * y + 1, color);
                    state_lock.image_buffer.put_pixel(3 * x + 1, 3 * y + 2, color);
                }
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
    cdn_id: &'static AtomicUsize
) -> ! {
    let image = warp::path!("overlay.png").and(warp::get()).map(|| {
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
            reserve(x, y, sender, cdn_id);
            Response::new("")
        });

    let routes = warp::any().and(image.or(reserve)); //.with(cors).with(warp::log("cors test"));

    warp::serve(routes).run(([127, 0, 0, 1], 8000)).await;

    unreachable!()
}

fn reserve(x: u32, y: u32, sender: &UnboundedSender<Reservation>, cdn_id: &'static AtomicUsize) {
    sender.send(Reservation { x, y, index: cdn_id.load(Ordering::SeqCst) }).unwrap();
}

fn write_buf_to_vec(buf: &RgbaImage) -> Vec<u8> {
    let mut cursor = Cursor::new(Vec::new());
    buf.write_to(&mut cursor, ImageOutputFormat::Png).unwrap();
    cursor.into_inner()
}

async fn maintain_cdn_buf(
    state: &'static Mutex<State>,
    cdn_id: &'static AtomicUsize,
    image: &'static RwLock<Vec<u8>>,
) -> ! {
    refresh_cdn_image(state, cdn_id, image, true).await;
    let mut interval = tokio::time::interval(CDN_REFRESH);
    loop {
        interval.tick().await;
        refresh_cdn_image(state, cdn_id, image, false).await;
    }
}

async fn refresh_cdn_image(
    state: &'static Mutex<State>,
    cdn_id: &'static AtomicUsize,
    image: &'static RwLock<Vec<u8>>,
    force: bool,
) {
    let new_img_bytes = reqwest::get(CDN_URL).await.unwrap().bytes().await.unwrap();
    let cdn_buf: RgbaImage = image::load_from_memory(&new_img_bytes).unwrap().to_rgba8();

    let mut state_lock = state.lock().unwrap();
    if !force && state_lock.cdn_buffer == cdn_buf {
        return
    }

    cdn_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    state_lock.image_buffer = RgbaImage::new(cdn_buf.width(), cdn_buf.height());
    state_lock.cdn_buffer = cdn_buf;

    let vec = write_buf_to_vec(&state_lock.image_buffer);
    drop(state_lock);

    let mut image_lock = image.write().unwrap();
    *image_lock = vec;
    drop(image_lock);
}