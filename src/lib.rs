// Copyright 2026-present Orbit Contributors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Bounded Server-Sent Events framing and an HTTP-body implementation.
use std::{
    error::Error,
    fmt,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures_core::Stream;
use http::{Response, header};
use http_body::{Body, Frame, SizeHint};

pub const MAX_EVENT_BYTES: usize = 65_536;
pub const MAX_EVENT_NAME_BYTES: usize = 256;
pub const MAX_EVENT_ID_BYTES: usize = 1_024;
pub const MAX_COMMENT_BYTES: usize = 4_096;
pub const MAX_RETRY_MS: u32 = 2_147_483_647;

/// Sanitized SSE validation failure categories.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SseError {
    /// A field contains invalid text, separators, or an out-of-range value.
    InvalidField,
    /// The encoded event or comment exceeds its byte limit.
    TooLarge,
}

impl fmt::Display for SseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidField => "invalid SSE field",
            Self::TooLarge => "SSE frame exceeds configured limit",
        })
    }
}
impl Error for SseError {}

/// One bounded SSE event; payload and ID are omitted from debug output.
#[derive(Clone)]
pub struct Event {
    data: String,
    event: Option<String>,
    id: Option<String>,
    retry: Option<u32>,
}

impl Event {
    /// Create an event with application data.
    pub fn new(data: impl Into<String>) -> Result<Self, SseError> {
        let data = data.into();
        validate_text(&data, MAX_EVENT_BYTES, false, false)?;
        Ok(Self {
            data,
            event: None,
            id: None,
            retry: None,
        })
    }

    /// Set the event type, rejecting line breaks and oversized UTF-8 values.
    pub fn with_event(mut self, event: impl Into<String>) -> Result<Self, SseError> {
        let event = event.into();
        validate_text(&event, MAX_EVENT_NAME_BYTES, true, false)?;
        self.event = Some(event);
        Ok(self)
    }

    /// Set the reconnect ID, rejecting line breaks, NUL, and oversized values.
    pub fn with_id(mut self, id: impl Into<String>) -> Result<Self, SseError> {
        let id = id.into();
        validate_text(&id, MAX_EVENT_ID_BYTES, true, true)?;
        self.id = Some(id);
        Ok(self)
    }

    /// Set the reconnect delay in milliseconds.
    pub fn with_retry(mut self, retry_ms: u32) -> Result<Self, SseError> {
        if retry_ms > MAX_RETRY_MS {
            return Err(SseError::InvalidField);
        }
        self.retry = Some(retry_ms);
        Ok(self)
    }

    /// Encode the event using UTF-8 and the HTML event-stream line format.
    pub fn encode(&self) -> Result<Bytes, SseError> {
        let mut frame = String::new();
        if let Some(event) = &self.event {
            frame.push_str("event: ");
            frame.push_str(event);
            frame.push('\n');
        }
        if let Some(id) = &self.id {
            frame.push_str("id: ");
            frame.push_str(id);
            frame.push('\n');
        }
        if let Some(retry) = self.retry {
            frame.push_str(&format!("retry: {retry}\n"));
        }
        let normalized = self.data.replace("\r\n", "\n").replace('\r', "\n");
        for line in normalized.split('\n') {
            frame.push_str("data: ");
            frame.push_str(line);
            frame.push('\n');
        }
        frame.push('\n');
        if frame.len() > MAX_EVENT_BYTES {
            return Err(SseError::TooLarge);
        }
        Ok(Bytes::from(frame))
    }
}

impl fmt::Debug for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Event")
            .field("event_set", &self.event.is_some())
            .field("data_bytes", &self.data.len())
            .field("id_set", &self.id.is_some())
            .field("retry", &self.retry)
            .finish()
    }
}

/// Encode a bounded SSE comment, commonly used as a caller-managed keepalive.
pub fn encode_comment(comment: &str) -> Result<Bytes, SseError> {
    validate_text(comment, MAX_COMMENT_BYTES, false, false)?;
    let normalized = comment.replace("\r\n", "\n").replace('\r', "\n");
    let mut frame = String::new();
    for line in normalized.split('\n') {
        frame.push_str(": ");
        frame.push_str(line);
        frame.push('\n');
    }
    frame.push('\n');
    if frame.len() > MAX_COMMENT_BYTES {
        return Err(SseError::TooLarge);
    }
    Ok(Bytes::from(frame))
}

fn validate_text(
    value: &str,
    maximum: usize,
    line_safe: bool,
    reject_null: bool,
) -> Result<(), SseError> {
    if value.len() > maximum
        || (line_safe && value.contains(['\r', '\n']))
        || (reject_null && value.contains('\0'))
    {
        return Err(SseError::InvalidField);
    }
    Ok(())
}

type BoxError = Box<dyn Error + Send + Sync + 'static>;

/// A pull-based body that serializes one event per poll and preserves upstream backpressure.
pub struct SseBody<S> {
    stream: Pin<Box<S>>,
    ended: bool,
}

impl<S> SseBody<S> {
    /// Wrap a stream of events and sanitized source errors without prefetching.
    pub fn new(stream: S) -> Self {
        Self {
            stream: Box::pin(stream),
            ended: false,
        }
    }
}

impl<S, E> Body for SseBody<S>
where
    S: Stream<Item = Result<Event, E>>,
    E: Error + Send + Sync + 'static,
{
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if self.ended {
            return Poll::Ready(None);
        }
        match self.stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                self.ended = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                self.ended = true;
                Poll::Ready(Some(Err(Box::new(error))))
            }
            Poll::Ready(Some(Ok(event))) => match event.encode() {
                Ok(bytes) => Poll::Ready(Some(Ok(Frame::data(bytes)))),
                Err(error) => {
                    self.ended = true;
                    Poll::Ready(Some(Err(Box::new(error))))
                }
            },
        }
    }

    fn is_end_stream(&self) -> bool {
        self.ended
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

/// Build an HTTP response with safe default SSE headers.
pub fn response<S, E>(stream: S) -> Result<Response<SseBody<S>>, http::Error>
where
    S: Stream<Item = Result<Event, E>>,
    E: Error + Send + Sync + 'static,
{
    Response::builder()
        .status(http::StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream; charset=utf-8")
        .header(header::CACHE_CONTROL, "no-cache")
        .header(
            header::HeaderName::from_static("x-content-type-options"),
            "nosniff",
        )
        .body(SseBody::new(stream))
}
