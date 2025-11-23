import sys
import json
import logging
import os
from pathlib import Path
import threading

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QFormLayout, QGroupBox,
    QLabel, QLineEdit, QPushButton, QComboBox, QCheckBox, QProgressBar,
    QTextEdit, QFileDialog, QMessageBox, QTabWidget, QTableWidget, QTableWidgetItem,
    QDialog, QListWidget, QListWidgetItem, QInputDialog, QMenu, QHeaderView, QFrame
)
from PySide6.QtCore import QThread, Signal, Slot, Qt, QSize
from PySide6.QtGui import QColor, QAction, QKeySequence, QActionGroup, QFont, QIcon

import qtawesome as qta
import file_organizer

BASE_DIR = Path(__file__).parent
SETTINGS_FILE = BASE_DIR / "settings.json"
PROFILES_FILE = BASE_DIR / "profiles.json"

# --- Modern Stylesheet ---
MODERN_STYLESHEET = """
QMainWindow {
    background-color: #1e293b;
    color: #e2e8f0;
}
QWidget {
    font-family: 'Segoe UI', 'Roboto', sans-serif;
    font-size: 14px;
    color: #e2e8f0;
}
QGroupBox {
    background-color: #334155;
    border: 1px solid #475569;
    border-radius: 10px;
    margin-top: 24px;
    padding-top: 15px;
    font-weight: bold;
}
QGroupBox::title {
    subcontrol-origin: margin;
    subcontrol-position: top left;
    padding: 0 10px;
    background-color: #334155; 
    border-radius: 5px;
    color: #38bdf8; /* Cyan title */
}
QLineEdit {
    background-color: #0f172a;
    border: 1px solid #475569;
    border-radius: 6px;
    padding: 8px;
    color: #f8fafc;
    selection-background-color: #38bdf8;
}
QLineEdit:focus {
    border: 1px solid #38bdf8;
}
QComboBox {
    background-color: #0f172a;
    border: 1px solid #475569;
    border-radius: 6px;
    padding: 6px;
    min-width: 6em;
}
QComboBox::drop-down {
    subcontrol-origin: padding;
    subcontrol-position: top right;
    width: 20px;
    border-left-width: 0px;
    border-top-right-radius: 6px;
    border-bottom-right-radius: 6px;
}
QPushButton {
    background-color: #475569;
    color: white;
    border-radius: 6px;
    padding: 8px 16px;
    font-weight: bold;
    border: none;
}
QPushButton:hover {
    background-color: #64748b;
}
QPushButton:pressed {
    background-color: #334155;
}
/* Primary Action Button (Run) */
QPushButton#btn_run {
    background-color: #10b981; /* Emerald Green */
    font-size: 15px;
}
QPushButton#btn_run:hover {
    background-color: #059669;
}
/* Cancel/Destructive Button */
QPushButton#btn_cancel {
    background-color: #ef4444; /* Red */
}
QPushButton#btn_cancel:hover {
    background-color: #dc2626;
}
QTabWidget::pane {
    border: 1px solid #475569;
    border-radius: 6px;
    background-color: #1e293b;
}
QTabBar::tab {
    background: #334155;
    color: #94a3b8;
    padding: 10px 20px;
    margin-right: 2px;
    border-top-left-radius: 6px;
    border-top-right-radius: 6px;
}
QTabBar::tab:selected {
    background: #38bdf8;
    color: #0f172a;
    font-weight: bold;
}
QTableWidget {
    background-color: #0f172a;
    gridline-color: #334155;
    border-radius: 6px;
    border: 1px solid #475569;
}
QHeaderView::section {
    background-color: #334155;
    padding: 6px;
    border: none;
    color: #e2e8f0;
    font-weight: bold;
}
QProgressBar {
    border: 1px solid #475569;
    border-radius: 6px;
    text-align: center;
    background-color: #0f172a;
}
QProgressBar::chunk {
    background-color: #38bdf8;
    border-radius: 5px;
}
QScrollBar:vertical {
    border: none;
    background: #0f172a;
    width: 10px;
    border-radius: 5px;
}
QScrollBar::handle:vertical {
    background: #475569;
    min-height: 20px;
    border-radius: 5px;
}
"""

class Translator:
    def __init__(self, lang="en"):
        self.lang = lang
        self.data = {"en": {}}
        try:
            with open(BASE_DIR / "translations.json", "r", encoding="utf-8") as f:
                self.data = json.load(f)
        except Exception as e:
            print(f"Warning: Could not load translations.json: {e}")
    
    def set_lang(self, lang):
        if lang in self.data:
            self.lang = lang
    
    def t(self, key, default=None):
        return self.data.get(self.lang, {}).get(key, self.data.get("en", {}).get(key, default or key))

class QtLogHandler(logging.Handler):
    def __init__(self, log_signal: Signal):
        super().__init__()
        self.log_signal = log_signal
    
    def emit(self, record):
        msg = self.format(record)
        self.log_signal.emit(msg)

class OrganizerWorker(QThread):
    progress_updated = Signal(int, int)
    result_logged = Signal(str, str, str, str)
    scan_finished = Signal(int)
    finished = Signal(dict, bool)
    log_message = Signal(str)
    
    def __init__(self, params: dict):
        super().__init__()
        self.params = params
    
    def run(self):
        try:
            logger = logging.getLogger("file_organizer")
            for h in logger.handlers[:]:
                logger.removeHandler(h)
            handler = QtLogHandler(self.log_message)
            handler.setFormatter(logging.Formatter("%(message)s"))
            logger.addHandler(handler)
            
            files = file_organizer.list_files(
                self.params["source"],
                self.params["recursive"],
                self.params["dest"]
            )
            self.scan_finished.emit(len(files))
            
            def on_progress_callback(i, total, file, result):
                if self.params['cancel_event'].is_set():
                    raise InterruptedError("Cancelled by user")
                
                status_map = {True: "Success", False: "Failed", None: "Skipped"}
                status = status_map.get(result, "Unknown")
                dest_path = "N/A"
                
                if status == "Success":
                    try:
                        if file_organizer.UNDO_LOG_FILE.exists() and file_organizer.UNDO_LOG_FILE.stat().st_size > 0:
                            with open(file_organizer.UNDO_LOG_FILE, 'rb') as f:
                                f.seek(0, os.SEEK_END)
                                file_size = f.tell()
                                if file_size > 1:
                                    f.seek(-2, os.SEEK_END)
                                    while f.tell() > 0 and f.read(1) != b'\n':
                                        f.seek(-2, os.SEEK_CUR)
                                    last_line = f.readline().decode()
                                    parts = last_line.strip().split('|')
                                    if len(parts) >= 3:
                                        dest_path = parts[-1]
                                else:
                                    f.seek(0)
                                    last_line = f.readline().decode()
                                    parts = last_line.strip().split('|')
                                    if len(parts) >= 3:
                                        dest_path = parts[-1]
                    except Exception:
                        dest_path = "..."
                
                self.result_logged.emit(str(file.resolve()), dest_path, file.name, status)
                self.progress_updated.emit(i, total)
            
            self.params['on_progress'] = on_progress_callback
            self.params['files'] = files
            stats = file_organizer.process_directory(**self.params)
            self.finished.emit(stats, self.params['cancel_event'].is_set())
        except Exception as e:
            self.log_message.emit(f"FATAL ERROR: {e}")
            self.finished.emit({}, False)
    
    def cancel(self):
        if 'cancel_event' in self.params:
            self.params['cancel_event'].set()

class UndoWorker(QThread):
    progress_updated = Signal(int, int)
    finished = Signal(dict)
    log_message = Signal(str)

    def run(self):
        logger = logging.getLogger("file_organizer")
        for h in logger.handlers[:]:
            logger.removeHandler(h)
        handler = QtLogHandler(self.log_message)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        
        def on_progress_callback(current, total):
            self.progress_updated.emit(current, total)
        
        stats = file_organizer.perform_undo(on_progress=on_progress_callback)
        self.finished.emit(stats)

class CategoryEditorDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.tr = parent.tr
        self.setWindowTitle(self.tr.t("manage_categories"))
        self.setMinimumSize(700, 500)
        self.setStyleSheet(parent.styleSheet()) # Inherit modern style
        self.categories_data = {k: list(v) for k, v in file_organizer.load_categories().items()}
        
        layout = QHBoxLayout(self)
        
        cat_layout = QVBoxLayout()
        cat_layout.addWidget(QLabel(self.tr.t("categories")))
        self.cat_list = QListWidget()
        self.cat_list.itemSelectionChanged.connect(self.update_ext_list)
        cat_layout.addWidget(self.cat_list)
        
        cat_btn_layout = QHBoxLayout()
        self.btn_add_cat = QPushButton(self.tr.t("add"))
        self.btn_add_cat.clicked.connect(self.add_category)
        self.btn_rename_cat = QPushButton(self.tr.t("rename"))
        self.btn_rename_cat.clicked.connect(self.rename_category)
        self.btn_remove_cat = QPushButton(self.tr.t("remove"))
        self.btn_remove_cat.clicked.connect(self.remove_category)
        cat_btn_layout.addWidget(self.btn_add_cat)
        cat_btn_layout.addWidget(self.btn_rename_cat)
        cat_btn_layout.addWidget(self.btn_remove_cat)
        cat_layout.addLayout(cat_btn_layout)
        layout.addLayout(cat_layout)
        
        ext_layout = QVBoxLayout()
        ext_layout.addWidget(QLabel(self.tr.t("extensions")))
        self.ext_list = QListWidget()
        ext_layout.addWidget(self.ext_list)
        
        ext_btn_layout = QHBoxLayout()
        self.btn_add_ext = QPushButton(self.tr.t("add"))
        self.btn_add_ext.clicked.connect(self.add_extension)
        self.btn_remove_ext = QPushButton(self.tr.t("remove"))
        self.btn_remove_ext.clicked.connect(self.remove_extension)
        ext_btn_layout.addWidget(self.btn_add_ext)
        ext_btn_layout.addWidget(self.btn_remove_ext)
        ext_layout.addLayout(ext_btn_layout)
        layout.addLayout(ext_layout)
        
        main_v_layout = QVBoxLayout()
        main_v_layout.addLayout(layout)
        self.btn_save = QPushButton(qta.icon('fa5s.save', color='white'), self.tr.t("save_and_close"))
        self.btn_save.setStyleSheet("background-color: #10b981;") 
        self.btn_save.clicked.connect(self.save_and_accept)
        main_v_layout.addWidget(self.btn_save)
        self.setLayout(main_v_layout)
        self.populate_cat_list()
    
    def populate_cat_list(self):
        self.cat_list.clear()
        for cat in sorted(self.categories_data.keys()):
            self.cat_list.addItem(QListWidgetItem(cat))
    
    def update_ext_list(self):
        self.ext_list.clear()
        selected = self.cat_list.selectedItems()
        if not selected:
            return
        cat = selected[0].text()
        for ext in sorted(self.categories_data.get(cat, [])):
            self.ext_list.addItem(QListWidgetItem(ext))
    
    def add_category(self):
        text, ok = QInputDialog.getText(self, self.tr.t("add_category"), self.tr.t("category_name"))
        if ok and text and text not in self.categories_data:
            self.categories_data[text] = []
            self.populate_cat_list()
    
    def rename_category(self):
        selected = self.cat_list.selectedItems()
        if not selected:
            return
        old = selected[0].text()
        new, ok = QInputDialog.getText(self, self.tr.t("rename_category"), self.tr.t("new_name"), text=old)
        if ok and new and new != old:
            self.categories_data[new] = self.categories_data.pop(old)
            self.populate_cat_list()
    
    def remove_category(self):
        selected = self.cat_list.selectedItems()
        if not selected:
            return
        cat = selected[0].text()
        if QMessageBox.question(
            self,
            self.tr.t("confirm_delete"),
            self.tr.t("confirm_delete_cat_msg").format(cat)
        ) == QMessageBox.StandardButton.Yes:
            del self.categories_data[cat]
            self.populate_cat_list()
            self.ext_list.clear()
    
    def add_extension(self):
        selected = self.cat_list.selectedItems()
        if not selected:
            return
        cat = selected[0].text()
        text, ok = QInputDialog.getText(self, self.tr.t("add_extension"), self.tr.t("extension_name"))
        if ok and text:
            ext = text.lower() if text.startswith('.') else '.' + text.lower()
            if ext not in self.categories_data[cat]:
                self.categories_data[cat].append(ext)
                self.update_ext_list()
    
    def remove_extension(self):
        cat_sel = self.cat_list.selectedItems()
        ext_sel = self.ext_list.selectedItems()
        if not cat_sel or not ext_sel:
            return
        cat = cat_sel[0].text()
        ext = ext_sel[0].text()
        self.categories_data[cat].remove(ext)
        self.update_ext_list()
    
    def save_and_accept(self):
        try:
            with open(file_organizer.CATEGORIES_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.categories_data, f, indent=2, sort_keys=True)
            QMessageBox.information(self, self.tr.t("success"), self.tr.t("categories_saved"))
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, self.tr.t("error"), f"{self.tr.t('categories_save_error')}\n{e}")

class PathLineEdit(QLineEdit):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setDragEnabled(True)
        self.setAcceptDrops(True)
        self.setPlaceholderText("C:/Example/Folder")
    
    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
    
    def dropEvent(self, event):
        url = event.mimeData().urls()[0]
        if url.isLocalFile():
            self.setText(url.toLocalFile())

class ManageProfilesDialog(QDialog):
    def __init__(self, profiles, parent=None):
        super().__init__(parent)
        self.tr = parent.tr
        self.profiles = profiles
        self.setWindowTitle(self.tr.t("manage_profiles_title"))
        self.setStyleSheet(parent.styleSheet())
        
        layout = QVBoxLayout(self)
        self.profile_list = QListWidget()
        self.profile_list.addItems(self.profiles.keys())
        layout.addWidget(self.profile_list)
        
        btn_layout = QHBoxLayout()
        remove_btn = QPushButton(self.tr.t("remove"))
        remove_btn.setObjectName("btn_cancel") # Red button
        remove_btn.clicked.connect(self.remove_profile)
        btn_layout.addStretch()
        btn_layout.addWidget(remove_btn)
        layout.addLayout(btn_layout)
    
    def remove_profile(self):
        selected = self.profile_list.currentItem()
        if not selected:
            return
        name = selected.text()
        if QMessageBox.question(
            self,
            self.tr.t("confirm_delete"),
            self.tr.t("confirm_delete_profile_msg").format(name)
        ) == QMessageBox.StandardButton.Yes:
            del self.profiles[name]
            self.profile_list.takeItem(self.profile_list.row(selected))
            self.parent()._save_profiles()
            self.parent()._update_profiles_menu()

class FileOrganizerGUI(QMainWindow):
    log_signal = Signal(str)
    
    def __init__(self):
        super().__init__()
        # Apply the modern stylesheet globally
        QApplication.instance().setStyleSheet(MODERN_STYLESHEET)
        
        self.tr = Translator("en")
        self.organizer_worker = None
        self.undo_worker = None
        self.profiles = {}
        self.current_theme = "modern" # Fixed modern theme
        self.load_profiles()
        self._setup_combo_boxes()
        self._create_actions()
        self._create_main_layout()
        self._create_menu_bar()
        self._create_tool_bar()
        self._create_status_bar()
        self.connect_signals()
        self.load_settings()
        self.change_lang()

    def _setup_combo_boxes(self):
        self.modes = {
            "type": "mode_type",
            "name": "mode_name",
            "date": "mode_date",
            "day": "mode_day",
            "size": "mode_size",
            "first_letter": "mode_first_letter"
        }
        self.mode_tooltips = {
            "type": "mode_type_desc",
            "name": "mode_name_desc",
            "date": "mode_date_desc",
            "day": "mode_day_desc",
            "size": "mode_size_desc",
            "first_letter": "mode_first_letter_desc"
        }
        self.actions = {"move": "action_move", "copy": "action_copy"}
        self.conflicts = {"rename": "conflict_rename", "skip": "conflict_skip", "overwrite": "conflict_overwrite"}

    def _populate_combobox(self, combo: QComboBox, data: dict, tooltips: dict = None):
        current_data = combo.currentData()
        combo.clear()
        for i, (key, trans_key) in enumerate(data.items()):
            combo.addItem(self.tr.t(trans_key), userData=key)
            if tooltips and key in tooltips:
                combo.setItemData(i, self.tr.t(tooltips[key]), Qt.ToolTipRole)
        index = combo.findData(current_data)
        if index != -1:
            combo.setCurrentIndex(index)

    def _create_actions(self):
        self.run_action = QAction(qta.icon('fa5s.play', color='#10b981'), "", self)
        self.run_action.setShortcut(QKeySequence("Ctrl+R"))
        self.cancel_action = QAction(qta.icon('fa5s.stop-circle', color='#ef4444'), "", self)
        self.cancel_action.setEnabled(False)
        self.open_dest_action = QAction(qta.icon('fa5s.folder-open', color='#38bdf8'), "", self)
        self.manage_cat_action = QAction(qta.icon('fa5s.cogs', color='#e2e8f0'), "", self)
        self.undo_action = QAction(qta.icon('fa5s.undo', color='#fbbf24'), "", self)
        self.exit_action = QAction(qta.icon('fa5s.times-circle', color='#ef4444'), "", self)
        self.exit_action.setShortcut(QKeySequence("Ctrl+Q"))
        self.save_profile_action = QAction(qta.icon('fa5s.save', color='#e2e8f0'), "", self)
        self.manage_profiles_action = QAction(qta.icon('fa5s.tasks', color='#e2e8f0'), "", self)
        self.schedule_action = QAction(qta.icon('fa5s.clock', color='#e2e8f0'), "", self)

    def _create_main_layout(self):
        self.resize(900, 750)
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(15)
        main_layout.setContentsMargins(20, 20, 20, 20)
        
        # --- Header ---
        top_layout = QHBoxLayout()
        title_lbl = QLabel("📁 File Organizer Pro")
        title_lbl.setStyleSheet("font-size: 20px; font-weight: bold; color: #38bdf8;")
        top_layout.addWidget(title_lbl)
        
        top_layout.addStretch()
        self.lbl_lang = QLabel()
        self.cmb_lang = QComboBox()
        self.cmb_lang.addItems(list(self.tr.data.keys()))
        self.cmb_lang.setFixedWidth(100)
        top_layout.addWidget(self.lbl_lang)
        top_layout.addWidget(self.cmb_lang)
        main_layout.addLayout(top_layout)
        
        # --- Paths Card ---
        paths_group = QGroupBox()
        main_layout.addWidget(paths_group)
        paths_layout = QFormLayout(paths_group)
        paths_layout.setSpacing(12)
        
        self.txt_source = PathLineEdit()
        self.btn_browse_source = QPushButton(qta.icon('fa5s.search', color='white'), "")
        self.btn_browse_source.setFixedWidth(40)
        source_layout = QHBoxLayout()
        source_layout.addWidget(self.txt_source)
        source_layout.addWidget(self.btn_browse_source)
        self.lbl_source = QLabel()
        paths_layout.addRow(self.lbl_source, source_layout)
        
        self.txt_dest = QLineEdit()
        self.txt_dest.setPlaceholderText("Optional (Default: Inside source folder)")
        self.btn_browse_dest = QPushButton(qta.icon('fa5s.search', color='white'), "")
        self.btn_browse_dest.setFixedWidth(40)
        dest_layout = QHBoxLayout()
        dest_layout.addWidget(self.txt_dest)
        dest_layout.addWidget(self.btn_browse_dest)
        self.lbl_dest = QLabel()
        paths_layout.addRow(self.lbl_dest, dest_layout)
        
        # --- Options Card ---
        options_group = QGroupBox()
        main_layout.addWidget(options_group)
        options_main_h = QHBoxLayout(options_group)
        
        # Left Side: Dropdowns
        form_layout = QFormLayout()
        form_layout.setHorizontalSpacing(20)
        form_layout.setVerticalSpacing(10)
        self.cmb_mode = QComboBox()
        self.lbl_mode = QLabel()
        form_layout.addRow(self.lbl_mode, self.cmb_mode)
        self.cmb_action = QComboBox()
        self.lbl_action = QLabel()
        form_layout.addRow(self.lbl_action, self.cmb_action)
        self.cmb_conflict = QComboBox()
        self.lbl_conflict = QLabel()
        form_layout.addRow(self.lbl_conflict, self.cmb_conflict)
        options_main_h.addLayout(form_layout, stretch=2)
        
        # Right Side: Checks & Big Buttons
        right_options_v = QVBoxLayout()
        self.chk_recursive = QCheckBox()
        self.chk_dryrun = QCheckBox()
        right_options_v.addWidget(self.chk_recursive)
        right_options_v.addWidget(self.chk_dryrun)
        right_options_v.addStretch()
        
        # Action Buttons Area
        actions_layout = QHBoxLayout()
        self.btn_run_big = QPushButton(qta.icon('fa5s.play', color='white'), " RUN")
        self.btn_run_big.setObjectName("btn_run") # Apply custom CSS ID
        self.btn_run_big.setMinimumHeight(45)
        self.btn_run_big.clicked.connect(self.run_organizer)
        
        self.btn_cancel_big = QPushButton(qta.icon('fa5s.stop', color='white'), "")
        self.btn_cancel_big.setObjectName("btn_cancel")
        self.btn_cancel_big.setMinimumHeight(45)
        self.btn_cancel_big.setFixedWidth(50)
        self.btn_cancel_big.clicked.connect(self.cancel_organizer)
        self.btn_cancel_big.setEnabled(False)
        
        actions_layout.addWidget(self.btn_run_big, stretch=1)
        actions_layout.addWidget(self.btn_cancel_big)
        right_options_v.addLayout(actions_layout)
        
        options_main_h.addLayout(right_options_v, stretch=1)
        
        # --- Tabs ---
        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)
        
        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setStyleSheet("border: none; background-color: #0f172a; font-family: Consolas, monospace;")
        
        self.table_view = QTableWidget()
        self.table_view.setColumnCount(3)
        self.table_view.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.table_view.setAlternatingRowColors(False) 
        self.table_view.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table_view.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table_view.customContextMenuRequested.connect(self._create_table_context_menu)
        self.table_view.verticalHeader().setVisible(False)
        
        self.tabs.addTab(self.log_view, qta.icon('fa5s.stream', color='#38bdf8'), "")
        self.tabs.addTab(self.table_view, qta.icon('fa5s.table', color='#38bdf8'), "")
        
        # Logger setup
        logger = logging.getLogger("file_organizer")
        logger.setLevel(logging.INFO)
        if logger.hasHandlers():
            logger.handlers.clear()
        logger.addHandler(QtLogHandler(self.log_signal))

    def _create_menu_bar(self):
        self.menu_bar = self.menuBar()
        self.menu_bar.setStyleSheet("background-color: #334155; color: white;")
        
        self.file_menu = self.menu_bar.addMenu("")
        self.file_menu.addAction(self.schedule_action)
        self.file_menu.addAction(self.open_dest_action)
        self.file_menu.addSeparator()
        self.file_menu.addAction(self.exit_action)
        
        self.edit_menu = self.menu_bar.addMenu("")
        self.edit_menu.addAction(self.manage_cat_action)
        self.edit_menu.addSeparator()
        self.edit_menu.addAction(self.undo_action)
        
        # Remove Theme menu since we are enforcing modern look
        self.view_menu = self.menu_bar.addMenu("") 
        
        self.profiles_menu = self.menu_bar.addMenu(qta.icon('fa5s.bookmark', color='#38bdf8'), "")
        self.profiles_menu.addAction(self.save_profile_action)
        self.profiles_menu.addAction(self.manage_profiles_action)
        self.profiles_menu.addSeparator()

    def _create_tool_bar(self):
        tool_bar = self.addToolBar("Main Toolbar")
        tool_bar.setMovable(False)
        tool_bar.setStyleSheet("background-color: #334155; border-bottom: 1px solid #475569; spacing: 10px; padding: 5px;")
        tool_bar.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextBesideIcon)
        
        # We have big buttons now, but keep toolbar for shortcuts
        tool_bar.addAction(self.open_dest_action)
        tool_bar.addAction(self.manage_cat_action)
        tool_bar.addAction(self.undo_action)
        
    def _create_status_bar(self):
        self.status_bar = self.statusBar()
        self.status_bar.setStyleSheet("background-color: #1e293b; color: #94a3b8;")
        self.lbl_status = QLabel()
        self.progress = QProgressBar()
        self.progress.setVisible(False)
        self.progress.setFixedWidth(200)
        self.status_bar.addWidget(self.lbl_status, 1)
        self.status_bar.addPermanentWidget(self.progress)
    
    def _create_table_context_menu(self, pos):
        item = self.table_view.itemAt(pos)
        if not item:
            return
        menu = QMenu()
        menu.setStyleSheet("background-color: #334155; color: white;")
        open_file_action = menu.addAction(self.tr.t("open_file"))
        open_folder_action = menu.addAction(self.tr.t("open_folder"))
        action = menu.exec(self.table_view.mapToGlobal(pos))
        if action == open_file_action:
            dest_path = self.table_view.item(item.row(), 1).data(Qt.UserRole)
            if dest_path and Path(dest_path).is_file():
                os.startfile(dest_path)
        elif action == open_folder_action:
            dest_path = self.table_view.item(item.row(), 1).data(Qt.UserRole)
            if dest_path:
                os.startfile(Path(dest_path).parent)

    def connect_signals(self):
        self.cmb_lang.currentTextChanged.connect(self.change_lang)
        self.btn_browse_source.clicked.connect(self.browse_source)
        self.btn_browse_dest.clicked.connect(self.browse_dest)
        self.log_signal.connect(self.log_view.append)
        self.run_action.triggered.connect(self.run_organizer)
        self.cancel_action.triggered.connect(self.cancel_organizer)
        self.open_dest_action.triggered.connect(self.open_dest)
        self.manage_cat_action.triggered.connect(self.open_category_editor)
        self.undo_action.triggered.connect(self.undo_operation)
        self.exit_action.triggered.connect(self.close)
        self.save_profile_action.triggered.connect(self.save_profile)
        self.manage_profiles_action.triggered.connect(self.manage_profiles)
        self.schedule_action.triggered.connect(self.show_schedule_info)

    @Slot()
    def change_lang(self):
        lang = self.cmb_lang.currentText()
        self.tr.set_lang(lang)
        self.setWindowTitle(self.tr.t("title"))
        self.lbl_lang.setText(self.tr.t("language"))
        self.findChildren(QGroupBox)[0].setTitle(self.tr.t("source_dest_title"))
        self.findChildren(QGroupBox)[1].setTitle(self.tr.t("options_title"))
        self.lbl_source.setText(self.tr.t("source"))
        self.txt_source.setToolTip(self.tr.t("source_tooltip"))
        self.lbl_dest.setText(self.tr.t("destination"))
        self.txt_dest.setToolTip(self.tr.t("dest_tooltip"))
        self.lbl_mode.setText(self.tr.t("mode"))
        self.lbl_action.setText(self.tr.t("action"))
        self.lbl_conflict.setText(self.tr.t("conflict"))
        self.chk_recursive.setText(self.tr.t("recursive"))
        self.chk_dryrun.setText(self.tr.t("dry_run"))
        self._populate_combobox(self.cmb_mode, self.modes, self.mode_tooltips)
        self._populate_combobox(self.cmb_action, self.actions)
        self._populate_combobox(self.cmb_conflict, self.conflicts)
        self.cmb_mode.setToolTip(self.tr.t("mode_tooltip"))
        self.cmb_action.setToolTip(self.tr.t("action_tooltip"))
        self.cmb_conflict.setToolTip(self.tr.t("conflict_tooltip"))
        self.tabs.setTabText(0, self.tr.t("log"))
        self.tabs.setTabText(1, self.tr.t("results"))
        self.table_view.setHorizontalHeaderLabels([self.tr.t("original_file"), self.tr.t("new_path"), self.tr.t("status")])
        self.run_action.setText(self.tr.t("run"))
        self.btn_run_big.setText(" " + self.tr.t("run"))
        self.cancel_action.setText(self.tr.t("cancel"))
        self.open_dest_action.setText(self.tr.t("open_dest"))
        self.manage_cat_action.setText(self.tr.t("manage_categories"))
        self.undo_action.setText(self.tr.t("undo"))
        self.exit_action.setText(self.tr.t("exit"))
        self.file_menu.setTitle(self.tr.t("file_menu"))
        self.edit_menu.setTitle(self.tr.t("edit_menu"))
        self.view_menu.setTitle(self.tr.t("view_menu"))
        self.profiles_menu.setTitle(self.tr.t("profiles_menu"))
        self.save_profile_action.setText(self.tr.t("save_profile"))
        self.manage_profiles_action.setText(self.tr.t("manage_profiles"))
        self.schedule_action.setText(self.tr.t("schedule"))
        self.lbl_status.setText(self.tr.t("ready"))
        self._update_profiles_menu()
        
        # Update alignment for Arabic
        if lang == "ar":
            self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        else:
            self.setLayoutDirection(Qt.LayoutDirection.LeftToRight)

    def set_controls_enabled(self, enabled):
        self.run_action.setEnabled(enabled)
        self.btn_run_big.setEnabled(enabled)
        
        # Toggle Cancel button state
        self.cancel_action.setEnabled(not enabled and self.organizer_worker is not None and self.organizer_worker.isRunning())
        self.btn_cancel_big.setEnabled(not enabled and self.organizer_worker is not None and self.organizer_worker.isRunning())
        
        for group in self.centralWidget().findChildren(QGroupBox):
            group.setEnabled(enabled)
        self.menu_bar.setEnabled(enabled)

    @Slot(str, str, str, str)
    def on_result_logged(self, src_path, dest_path, display_name, status):
        row = self.table_view.rowCount()
        self.table_view.insertRow(row)
        item_name = QTableWidgetItem(display_name)
        item_name.setData(Qt.UserRole, src_path)
        item_name.setForeground(QColor("#e2e8f0"))
        
        item_dest = QTableWidgetItem(dest_path if status == "Success" else "N/A")
        item_dest.setData(Qt.UserRole, dest_path)
        item_dest.setForeground(QColor("#94a3b8"))
        
        status_item = QTableWidgetItem(status)
        if status == "Success":
            status_item.setForeground(QColor("#10b981")) # Green
        elif status == "Failed":
            status_item.setForeground(QColor("#ef4444")) # Red
        elif status == "Skipped":
            status_item.setForeground(QColor("#fbbf24")) # Yellow/Orange
            
        self.table_view.setItem(row, 0, item_name)
        self.table_view.setItem(row, 1, item_dest)
        self.table_view.setItem(row, 2, status_item)
        self.table_view.scrollToBottom()

    def load_profiles(self):
        if not PROFILES_FILE.exists():
            self.profiles = {}
            return
        try:
            with open(PROFILES_FILE, 'r', encoding='utf-8') as f:
                self.profiles = json.load(f)
        except json.JSONDecodeError:
            self.profiles = {}
    
    def _save_profiles(self):
        with open(PROFILES_FILE, 'w', encoding='utf-8') as f:
            json.dump(self.profiles, f, indent=2)
    
    def _update_profiles_menu(self):
        for action in self.profiles_menu.actions()[3:]:
            self.profiles_menu.removeAction(action)
        for name in sorted(self.profiles.keys()):
            action = QAction(name, self)
            action.triggered.connect(lambda checked, n=name: self.load_profile(n))
            self.profiles_menu.addAction(action)
    
    @Slot()
    def save_profile(self):
        name, ok = QInputDialog.getText(self, self.tr.t("save_profile"), self.tr.t("profile_name_prompt"))
        if ok and name:
            self.profiles[name] = self._get_current_settings_dict()
            self._save_profiles()
            self._update_profiles_menu()
    
    def load_profile(self, name):
        settings = self.profiles.get(name)
        if settings:
            self._apply_settings_dict(settings)
    
    @Slot()
    def manage_profiles(self):
        dialog = ManageProfilesDialog(self.profiles, self)
        dialog.exec()
    
    @Slot()
    def show_schedule_info(self):
        QMessageBox.information(self, self.tr.t("schedule_info_title"), self.tr.t("schedule_info_msg"))

    def _get_current_settings_dict(self):
        return {
            "source": self.txt_source.text(),
            "dest": self.txt_dest.text(),
            "mode": self.cmb_mode.currentData(),
            "action": self.cmb_action.currentData(),
            "conflict": self.cmb_conflict.currentData(),
            "recursive": self.chk_recursive.isChecked(),
            "dry_run": self.chk_dryrun.isChecked()
        }
    
    def _apply_settings_dict(self, s: dict):
        self.txt_source.setText(s.get("source", ""))
        self.txt_dest.setText(s.get("dest", ""))
        mode_index = self.cmb_mode.findData(s.get("mode"))
        action_index = self.cmb_action.findData(s.get("action"))
        conflict_index = self.cmb_conflict.findData(s.get("conflict"))
        if mode_index != -1:
            self.cmb_mode.setCurrentIndex(mode_index)
        if action_index != -1:
            self.cmb_action.setCurrentIndex(action_index)
        if conflict_index != -1:
            self.cmb_conflict.setCurrentIndex(conflict_index)
        self.chk_recursive.setChecked(s.get("recursive", True))
        self.chk_dryrun.setChecked(s.get("dry_run", False))

    def save_settings(self):
        try:
            settings = self._get_current_settings_dict()
            settings["lang"] = self.cmb_lang.currentText()
            with open(SETTINGS_FILE, 'w', encoding='utf-8') as f:
                json.dump(settings, f)
        except Exception as e:
            print(f"Could not save settings: {e}")
    
    def load_settings(self):
        if not SETTINGS_FILE.exists():
            self.chk_recursive.setChecked(True)
            return
        try:
            with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
                s = json.load(f)
            self._apply_settings_dict(s)
            self.cmb_lang.setCurrentText(s.get("lang", "en"))
        except Exception as e:
            print(f"Could not load settings: {e}")
    
    @Slot()
    def open_category_editor(self):
        dialog = CategoryEditorDialog(self)
        dialog.exec()
    
    @Slot()
    def run_organizer(self):
        source_text = self.txt_source.text().strip()
        if not source_text:
            QMessageBox.warning(self, self.tr.t("error"), self.tr.t("invalid_source"))
            return
        source = Path(source_text)
        dest_text = self.txt_dest.text().strip()
        dest = Path(dest_text) if dest_text else (source / "Organized_Files")
        if not source.is_dir():
            QMessageBox.warning(self, self.tr.t("error"), self.tr.t("invalid_source"))
            return
        
        file_organizer.clear_undo_log()
        self.table_view.setRowCount(0)
        self.log_view.clear()
        self.set_controls_enabled(False)
        self.lbl_status.setText(self.tr.t("scanning"))
        self.progress.setVisible(True)
        self.progress.setRange(0, 0)
        self.tabs.setCurrentIndex(0)
        
        params = {
            "source": source,
            "dest": dest,
            "mode": self.cmb_mode.currentData(),
            "action": self.cmb_action.currentData(),
            "dry_run": self.chk_dryrun.isChecked(),
            "recursive": self.chk_recursive.isChecked(),
            "conflict_policy": self.cmb_conflict.currentData(),
            "categories": file_organizer.load_categories(),
            "cancel_event": threading.Event()
        }
        self._last_dest = dest
        self.organizer_worker = OrganizerWorker(params)
        self.organizer_worker.scan_finished.connect(self.on_scan_finished)
        self.organizer_worker.result_logged.connect(self.on_result_logged)
        self.organizer_worker.progress_updated.connect(lambda i, t: self.progress.setValue(i))
        self.organizer_worker.finished.connect(self.on_worker_finished)
        self.organizer_worker.log_message.connect(self.log_view.append)
        self.organizer_worker.start()

    @Slot(int)
    def on_scan_finished(self, total):
        self.progress.setRange(0, total)
        self.lbl_status.setText(self.tr.t("starting"))
        self.table_view.setRowCount(0)
    
    @Slot(dict, bool)
    def on_worker_finished(self, stats, cancelled):
        self.progress.setVisible(False)
        if not cancelled:
            self.lbl_status.setText(self.tr.t("done"))
            self.tabs.setCurrentIndex(1)
            QMessageBox.information(self, self.tr.t("done"), self.tr.t("summary").format(**stats))
        else:
            self.lbl_status.setText(self.tr.t("cancelled"))
            QMessageBox.warning(self, self.tr.t("cancelled"), self.tr.t("summary").format(**stats))
        self.organizer_worker = None
        self.set_controls_enabled(True)
        self.table_view.resizeColumnsToContents()
    
    @Slot()
    def undo_operation(self):
        if QMessageBox.question(
            self,
            self.tr.t("confirm_undo"),
            self.tr.t("confirm_undo_msg")
        ) == QMessageBox.StandardButton.Yes:
            self.set_controls_enabled(False)
            self.lbl_status.setText(self.tr.t("starting"))
            self.progress.setVisible(True)
            self.progress.setValue(0)
            self.tabs.setCurrentIndex(0)

            self.undo_worker = UndoWorker()
            self.undo_worker.progress_updated.connect(self.on_undo_progress)
            self.undo_worker.finished.connect(self.on_undo_finished)
            self.undo_worker.log_message.connect(self.log_view.append)
            self.undo_worker.start()

    @Slot(int, int)
    def on_undo_progress(self, current, total):
        if self.progress.maximum() != total:
            self.progress.setRange(0, total)
        self.progress.setValue(current)

    @Slot(dict)
    def on_undo_finished(self, stats):
        self.progress.setVisible(False)
        self.lbl_status.setText(self.tr.t("ready"))
        QMessageBox.information(self, self.tr.t("undo_complete"), self.tr.t("undo_summary").format(**stats))
        self.undo_worker = None
        self.set_controls_enabled(True)

    def closeEvent(self, event):
        self.save_settings()
        event.accept()
    
    @Slot()
    def browse_source(self):
        folder = QFileDialog.getExistingDirectory(self)
        if folder:
            self.txt_source.setText(folder)
    
    @Slot()
    def browse_dest(self):
        folder = QFileDialog.getExistingDirectory(self)
        if folder:
            self.txt_dest.setText(folder)
    
    @Slot()
    def cancel_organizer(self):
        if self.organizer_worker:
            self.organizer_worker.cancel()
    
    @Slot()
    def open_dest(self):
        dest_text = self.txt_dest.text().strip()
        source_text = self.txt_source.text().strip()
        if not dest_text and not source_text:
            return
        dest = Path(dest_text) if dest_text else (Path(source_text) / "Organized_Files")
        if dest:
            try:
                dest.mkdir(parents=True, exist_ok=True)
                os.startfile(str(dest))
            except Exception as e:
                QMessageBox.warning(self, "Error", f"Could not open folder: {e}")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    # Set default font for the whole app
    font = QFont("Segoe UI", 10)
    app.setFont(font)
    
    window = FileOrganizerGUI()
    window.show()
    sys.exit(app.exec())
