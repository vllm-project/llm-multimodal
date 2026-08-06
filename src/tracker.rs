use std::{collections::HashMap, sync::Arc};

use tokio::task::JoinHandle;

use super::{
    error::{MultiModalError, MultiModalResult},
    media::{ImageFetchConfig, MediaConnector, MediaSource, VideoFetchConfig},
    types::{
        ImageDetail, MediaContentPart, Modality, MultiModalData, MultiModalUUIDs, TrackedMedia,
    },
};

type PendingTask = JoinHandle<MultiModalResult<TrackedMedia>>;

#[derive(Debug)]
pub struct TrackerOutput {
    pub data: MultiModalData,
    pub uuids: MultiModalUUIDs,
}

pub struct AsyncMultiModalTracker {
    media_connector: Arc<MediaConnector>,
    pending: HashMap<Modality, Vec<PendingTask>>,
    uuids: MultiModalUUIDs,
    video_config: VideoFetchConfig,
}

impl AsyncMultiModalTracker {
    pub fn new(media_connector: Arc<MediaConnector>) -> Self {
        Self {
            media_connector,
            pending: HashMap::new(),
            uuids: HashMap::new(),
            video_config: VideoFetchConfig::default(),
        }
    }

    /// Override the video decode settings used for every clip this tracker
    /// fetches.
    ///
    /// Callers that accept per-request media options (frame count, sampling
    /// rate) need this, since the tracker otherwise always decodes with
    /// [`VideoFetchConfig::default`].
    pub fn with_video_config(mut self, video_config: VideoFetchConfig) -> Self {
        self.video_config = video_config;
        self
    }

    pub fn push_part(&mut self, part: MediaContentPart) -> MultiModalResult<()> {
        match part {
            MediaContentPart::Text { .. } => {}
            MediaContentPart::ImageUrl { url, detail, uuid } => {
                let source = match url::Url::parse(&url) {
                    Ok(parsed) if parsed.scheme() == "data" => MediaSource::DataUrl(url),
                    _ => MediaSource::Url(url),
                };
                self.enqueue_image(source, detail.unwrap_or_default(), uuid);
            }
            MediaContentPart::ImageData {
                data,
                mime_type: _,
                uuid,
                detail,
            } => {
                self.enqueue_image(
                    MediaSource::InlineBytes(data),
                    detail.unwrap_or_default(),
                    uuid,
                );
            }
            MediaContentPart::ImageEmbeds { .. } => {
                return Err(MultiModalError::UnsupportedContent("image_embeds"));
            }
            MediaContentPart::AudioUrl { url, uuid } => {
                let source = match url::Url::parse(&url) {
                    Ok(parsed) if parsed.scheme() == "data" => MediaSource::DataUrl(url),
                    _ => MediaSource::Url(url),
                };
                self.enqueue_audio(source, uuid);
            }
            MediaContentPart::AudioData {
                data,
                mime_type: _,
                uuid,
            } => {
                self.enqueue_audio(MediaSource::InlineBytes(data), uuid);
            }
            MediaContentPart::VideoUrl { url, uuid } => {
                let source = match url::Url::parse(&url) {
                    Ok(parsed) if parsed.scheme() == "data" => MediaSource::DataUrl(url),
                    _ => MediaSource::Url(url),
                };
                self.enqueue_video(source, uuid);
            }
            MediaContentPart::VideoData {
                data,
                mime_type: _,
                uuid,
            } => {
                self.enqueue_video(MediaSource::InlineBytes(data), uuid);
            }
        }
        Ok(())
    }

    pub async fn finalize(mut self) -> MultiModalResult<TrackerOutput> {
        let mut data = MultiModalData::new();
        for (modality, tasks) in self.pending.drain() {
            let mut items = Vec::with_capacity(tasks.len());
            for task in tasks {
                let media = task.await??;
                items.push(media);
            }
            data.insert(modality, items);
        }

        Ok(TrackerOutput {
            data,
            uuids: self.uuids,
        })
    }

    fn enqueue_image(&mut self, source: MediaSource, detail: ImageDetail, uuid: Option<String>) {
        let modality = Modality::Image;
        self.uuids.entry(modality).or_default().push(uuid);

        let connector = Arc::clone(&self.media_connector);
        let handle = tokio::spawn(async move {
            let frame = connector
                .fetch_image(source, ImageFetchConfig { detail })
                .await?;
            Ok(TrackedMedia::Image(frame))
        });

        self.pending.entry(modality).or_default().push(handle);
    }

    fn enqueue_video(&mut self, source: MediaSource, uuid: Option<String>) {
        let modality = Modality::Video;
        self.uuids.entry(modality).or_default().push(uuid);

        let connector = Arc::clone(&self.media_connector);
        let video_config = self.video_config;
        let handle = tokio::spawn(async move {
            let clip = connector.fetch_video(source, video_config).await?;
            Ok(TrackedMedia::Video(clip))
        });

        self.pending.entry(modality).or_default().push(handle);
    }

    fn enqueue_audio(&mut self, source: MediaSource, uuid: Option<String>) {
        let modality = Modality::Audio;
        self.uuids.entry(modality).or_default().push(uuid);

        let connector = Arc::clone(&self.media_connector);
        let handle = tokio::spawn(async move {
            let clip = connector.fetch_audio(source).await?;
            Ok(TrackedMedia::Audio(clip))
        });

        self.pending.entry(modality).or_default().push(handle);
    }
}

#[cfg(test)]
mod video_config_tests {
    use super::*;

    #[test]
    fn tracker_defaults_to_the_library_video_config() {
        let connector =
            Arc::new(MediaConnector::new(reqwest::Client::new(), Default::default()).unwrap());
        let tracker = AsyncMultiModalTracker::new(connector);
        let default = VideoFetchConfig::default();

        assert_eq!(tracker.video_config.min_frames, default.min_frames);
        assert_eq!(tracker.video_config.max_frames, default.max_frames);
        assert_eq!(tracker.video_config.sample_fps, default.sample_fps);
    }

    #[test]
    fn with_video_config_overrides_every_field() {
        let connector =
            Arc::new(MediaConnector::new(reqwest::Client::new(), Default::default()).unwrap());
        let tracker = AsyncMultiModalTracker::new(connector).with_video_config(VideoFetchConfig {
            min_frames: 2,
            max_frames: 8,
            sample_fps: 0.5,
        });

        assert_eq!(tracker.video_config.min_frames, 2);
        assert_eq!(tracker.video_config.max_frames, 8);
        assert_eq!(tracker.video_config.sample_fps, 0.5);
    }
}
