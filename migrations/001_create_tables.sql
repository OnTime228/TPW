CREATE TABLE IF NOT EXISTS videos (
  id               TEXT PRIMARY KEY,
  creator_id       TEXT NOT NULL,
  video_created_at TIMESTAMPTZ NOT NULL,
  views_count      BIGINT NOT NULL DEFAULT 0,
  likes_count      BIGINT NOT NULL DEFAULT 0,
  comments_count   BIGINT NOT NULL DEFAULT 0,
  reports_count    BIGINT NOT NULL DEFAULT 0,
  created_at       TIMESTAMPTZ NOT NULL,
  updated_at       TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS video_snapshots (
  id                    TEXT PRIMARY KEY,
  video_id              TEXT NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
  views_count           BIGINT NOT NULL DEFAULT 0,
  likes_count           BIGINT NOT NULL DEFAULT 0,
  comments_count        BIGINT NOT NULL DEFAULT 0,
  reports_count         BIGINT NOT NULL DEFAULT 0,
  delta_views_count     BIGINT NOT NULL DEFAULT 0,
  delta_likes_count     BIGINT NOT NULL DEFAULT 0,
  delta_comments_count  BIGINT NOT NULL DEFAULT 0,
  delta_reports_count   BIGINT NOT NULL DEFAULT 0,
  created_at            TIMESTAMPTZ NOT NULL,
  updated_at            TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_videos_creator_id ON videos(creator_id);
CREATE INDEX IF NOT EXISTS idx_videos_video_created_at ON videos(video_created_at);

CREATE INDEX IF NOT EXISTS idx_snapshots_video_id ON video_snapshots(video_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_created_at ON video_snapshots(created_at);
CREATE INDEX IF NOT EXISTS idx_snapshots_delta_views ON video_snapshots(delta_views_count);