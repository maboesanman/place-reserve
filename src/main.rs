#![feature(map_first_last)]

use std::{sync::{RwLock, Mutex}, collections::BTreeMap, io::Cursor, time::Duration, ops::Add};
use futures::StreamExt;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use image::{RgbaImage, ImageOutputFormat, Pixel, Rgba};
use lazy_static::lazy_static;
use tokio::time::Instant;
use tokio_stream::wrappers::UnboundedReceiverStream;
use warp::{Filter, hyper::{Response, header::CONTENT_TYPE}, http::HeaderValue};
use serde::{Deserialize, Serialize};

struct Reservation {
    x: u16,
    y: u16,
    end: Instant,
}

#[derive(Deserialize)]
struct Query {
    x: u16, y: u16
}


#[tokio::main]
async fn main() {
    let (sender, receiver) = mpsc::unbounded_channel::<Reservation>();
    let sender = Box::leak(Box::new(sender));
    // let active_reservations: Mutex<BTreeMap<Instant, (u8, u8)>> = todo!();
    // let image_buffer: Image
    
    let mut image_buffer = RgbaImage::new(1000, 1000);
    let image: &'static RwLock<Vec<u8>> = {
        let mut cursor = Cursor::new(Vec::new());
        image_buffer.write_to(&mut cursor, ImageOutputFormat::Png);
        let vec = cursor.into_inner();
        
        Box::leak(Box::new(RwLock::new(vec)))
    };

    let state = State {
        image_buffer,
        active_reservations: BTreeMap::new(),
    };
    let state = Box::leak(Box::new(Mutex::new(state)));

    let add_fut = add_reservations(receiver, image, state);
    let exp_fut = expire_reservations(state, image);
    let web_fut = serve(sender, image);

    tokio::join!(add_fut, exp_fut, web_fut);
}

// async fn update_template_image()

struct State {
    pub image_buffer: RgbaImage,
    pub active_reservations: BTreeMap<Instant, (u16, u16)>,
}

async fn add_reservations(
    receiver: UnboundedReceiver<Reservation>,
    image: &'static RwLock<Vec<u8>>,
    state: &'static Mutex<State>
) -> ! {
    let receiver = UnboundedReceiverStream::new(receiver);
    let receiver_chunks = receiver.ready_chunks(1000);

    
    receiver_chunks.for_each(|chunk| {
        let mut state_lock = state.lock().unwrap();
        for res in chunk.into_iter() {
            let Reservation { x, y, end } = res;
            state_lock.active_reservations.insert(end, (x, y));
            state_lock.image_buffer.put_pixel(x.into(), y.into(), Rgba([0,0,0,255]));
        }
        let vec = write_buf_to_vec(&state_lock.image_buffer);
        drop(state_lock);

        let mut image_lock = image.write().unwrap();
        *image_lock = vec;
        drop(image_lock);


        futures::future::ready(())
    }).await;

    unreachable!()
}

async fn expire_reservations(
    state: &'static Mutex<State>,
    image: &'static RwLock<Vec<u8>>,
) -> ! {
    loop {
        let state_lock = state.lock().unwrap();
        
        if let Some((&next_instant, _)) = state_lock.active_reservations.first_key_value() {
            drop(state_lock);
            tokio::time::sleep_until(next_instant).await;

            let mut state_lock = state.lock().unwrap();
            let (_, (x, y)) = state_lock.active_reservations.pop_first().unwrap();
            state_lock.image_buffer.put_pixel(x.into(), y.into(), Rgba([0,0,0,0]));
            let vec = write_buf_to_vec(&state_lock.image_buffer);
            drop(state_lock);

            let mut image_lock = image.write().unwrap();
            *image_lock = vec;
            drop(image_lock);

        } else {
            drop(state_lock);
            tokio::time::sleep(Duration::new(5, 0)).await;
        }
    }
}

async fn serve(
    sender: &'static UnboundedSender<Reservation>,
    image: &'static RwLock<Vec<u8>>,
) -> ! {
    let image = warp::path!("image")
        .and(warp::get())
        .map(|| {
            let lock =  image.read().unwrap();
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
            let mut res = Response::new("");
            res
        });

    let routes = warp::any().and(
        image.or(reserve)
    );

    warp::serve(routes)
        .run(([127, 0, 0, 1], 8000))
        .await;

    unreachable!()
}

fn reserve(
    x: u16,
    y: u16,
    sender: &UnboundedSender<Reservation>
) {
    let _ = sender.send(Reservation {
        x,
        y,
        end: Instant::now().add(Duration::new(60 * 5, 0))
    });
}

fn write_buf_to_vec(buf: &RgbaImage) -> Vec<u8> {
    let mut cursor = Cursor::new(Vec::new());
    buf.write_to(&mut cursor, ImageOutputFormat::Png).unwrap();
    cursor.into_inner()
}

