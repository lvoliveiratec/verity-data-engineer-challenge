from pipeline.windowing import build_processing_window


def test_build_processing_window_is_inclusive():
    window = build_processing_window("2026-03-16", lookback_days=2)

    assert window.start_iso == "2026-03-14"
    assert window.end_iso == "2026-03-16"
