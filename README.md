# 📂 Sortify - Advanced File Organizer

A powerful and user-friendly desktop application built with Python and PySide6 to intelligently sort your files into clean, organized folders.

Tired of cluttered "Downloads" or "Desktop" folders? Sortify automates the cleaning process with a rich set of features, a multi-language interface, and robust safety mechanisms like Undo and Dry-run mode.

![Sortify Screenshot](https://raw.githubusercontent.com/abdulrahman20242/File-Organizer_v2.1/main/Capture.PNG)

---

## 🚀 Features

### Core Functionality
*   **Multiple Organization Modes:**
    *   **By Type:** Groups files into folders like `Images`, `Videos`, `Documents`.
    *   **By Date (Month):** Sorts files into `Year/Month` folders (e.g., `2024/10-October`).
    *   **By Date (Day):** Sorts files into `Year/Month/Day` folders (e.g., `2024/10/16`).
    *   **By Size:** Categorizes files as `Small`, `Medium`, or `Large`.
    *   **By First Letter:** Groups files into alphabetical folders (`A`, `B`, `C`...).
*   **Flexible Actions:** Choose to **Move** original files or create a **Copy**.
*   **Smart Conflict Resolution:** Automatically `Rename`, `Overwrite`, or `Skip` files if they already exist in the destination.
*   **Recursive Processing:** Option to include all files from subdirectories.

### User Experience & Interface
*   **✨ Graphical Category Editor:** An intuitive interface to add, rename, or remove categories and manage their file extensions **without ever touching a config file**.
*   **Modern GUI:** Clean and responsive interface built with PySide6.
*   **Real-time Progress:** A progress bar and live log ensure the app never freezes during long operations.
*   **Detailed Results Table:** See the status of each file (Success, Skipped, Failed) in a clear, color-coded table.
*   **Multi-language Support:** Switch between **English** and **Arabic** on the fly.
*   **Themes:** Instantly switch between **Light** and **Dark** modes.
*   **Drag & Drop:** Easily drop your source folder into the path input field.

### Safety & Customization
*   **↩️ Undo Last Operation:** A critical safety feature! Revert the entire last organization process with a single click.
*   **🛡️ Dry-run Mode:** A simulation mode that shows you what will happen **without touching your files**, allowing you to preview the result safely.
*   **💾 Profiles:** Save and load your favorite settings (e.g., "Sort Downloads" vs. "Backup Photos") for quick reuse.
*   **Easy Windows Launch:** Includes a `Sortify.bat` script for double-click execution.

---

## 🛠️ Installation

**Prerequisites:** Python 3.9+

1.  Clone the repository and navigate into the project directory:
    ```bash
    git clone https://github.com/abdulrahman20242/File-Organizer_v2.1.git
    cd File-Organizer_v2.1
    ```

2.  **Create and activate a virtual environment (Recommended):**
    *   On Windows:
        ```bash
        python -m venv venv
        .\venv\Scripts\activate
        ```
    *   On macOS/Linux:
        ```bash
        python3 -m venv venv
        source venv/bin/activate
        ```

3.  Install the required libraries using pip:
    ```bash
    pip install -r requirements.txt
    ```

---

## 🖥️ Usage

You can run the application in two ways:

1.  **From the terminal (all platforms):**
    (Make sure your virtual environment is active)
    ```bash
    python file_organizer_gui.py
    ```

2.  **On Windows (easy method):**
    Simply double-click the **`Sortify.bat`** file. This script automatically launches the application.

**How it works:**
1.  **Select Source & Destination:** Use the "Browse" buttons or drag-and-drop a folder. If the destination is empty, an `Organized_Files` folder will be created inside the source.
2.  **Choose Your Options:** Select the organization mode, action (move/copy), and conflict policy.
3.  **Run:** Click the "Run" button to start.
4.  **Monitor:** Watch the progress bar and view live logs or the color-coded results table.
5.  **Undo (if needed):** If you're not happy, just click "Undo".

---

## ⚙️ Customization

The easiest way to customize file categories is through the built-in **Category Editor**. You can access it from the `Edit -> Manage Categories` menu or the toolbar.

For advanced users, you can also manually edit the **`categories.json`** file. For example, to add `.eps` files to the "Images" category:
```json
{
  "Images": [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".webp", ".heic", ".eps"],
  "Videos": [...]
}
```

---

## 🧪 Running Tests

This project includes a comprehensive test suite to ensure the core logic is working correctly.

1.  Install the development dependencies:
    ```bash
    pip install -r requirements-dev.txt
    ```
2.  Run pytest from the project's root directory:
    ```bash
    pytest -v
    ```

---

## 📒 Project Structure

*   `file_organizer_gui.py`: The main file for the PySide6 graphical user interface.
*   `file_organizer.py`: Contains all the backend logic for file operations (sorting, undo, etc.).
*   `test_organizer.py`: The `pytest` test suite for the backend logic.
*   `Sortify.bat`: A convenience script for launching the GUI on Windows.
*   `translations.json`: Stores text strings for multi-language support.
*   `categories.json`: Default and user-customizable file type categories.
*   `requirements.txt`: Main dependencies required to run the application.
*   `requirements-dev.txt`: Extra dependencies for development and testing.
