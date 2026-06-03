from __future__ import annotations


def test_streamlit_app_module_imports() -> None:
    import bagelquant_data.gui.app as app

    assert app.main is not None
