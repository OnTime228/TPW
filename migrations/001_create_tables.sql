-- Schema for Scarecrow stats bot

CREATE TABLE IF NOT EXISTS videos (
    id UUID PRIMARY KEY,
    creator_id TEXT NOT NULL,
    video_created_at TIMESTAMPTZ NOT NULL,
    views_count BIGINT NOT NULL,
    likes_count BIGINT NOT NULL,
    comments_count BIGINT NOT NULL,
    reports_count BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_videos_creator_created_at ON videos (creator_id, video_created_at);
CREATE INDEX IF NOT EXISTS idx_videos_video_created_at ON videos (video_created_at);
CREATE INDEX IF NOT EXISTS idx_videos_views_count ON videos (views_count);
CREATE INDEX IF NOT EXISTS idx_videos_likes_count ON videos (likes_count);
CREATE INDEX IF NOT EXISTS idx_videos_comments_count ON videos (comments_count);
CREATE INDEX IF NOT EXISTS idx_videos_reports_count ON videos (reports_count);

CREATE TABLE IF NOT EXISTS video_snapshots (
    id UUID PRIMARY KEY,
    video_id UUID NOT NULL REFERENCES videos(id) ON DELETE CASCADE,

    views_count BIGINT NOT NULL,
    likes_count BIGINT NOT NULL,
    comments_count BIGINT NOT NULL,
    reports_count BIGINT NOT NULL,

    delta_views_count BIGINT NOT NULL,
    delta_likes_count BIGINT NOT NULL,
    delta_comments_count BIGINT NOT NULL,
    delta_reports_count BIGINT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_video_snapshots_created_at ON video_snapshots (created_at);
CREATE INDEX IF NOT EXISTS idx_video_snapshots_video_id_created_at ON video_snapshots (video_id, created_at);
CREATE INDEX IF NOT EXISTS idx_video_snapshots_delta_views ON video_snapshots (delta_views_count);
CREATE INDEX IF NOT EXISTS idx_video_snapshots_delta_likes ON video_snapshots (delta_likes_count);
CREATE INDEX IF NOT EXISTS idx_video_snapshots_delta_comments ON video_snapshots (delta_comments_count);
CREATE INDEX IF NOT EXISTS idx_video_snapshots_delta_reports ON video_snapshots (delta_reports_count);
