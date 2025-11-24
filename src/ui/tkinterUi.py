import os
import subprocess
import threading
import customtkinter as ctk
from tkinter import messagebox

# Setup paths
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))

# Setup CTk
ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")

app = ctk.CTk()
app.title("Dynamic Lands Lab: Script Runner")
app.geometry("400x400")

frame = ctk.CTkFrame(master=app)
frame.pack(padx=20, pady=20, fill="both", expand=True)

# Progress bar (spinner style)
progress = ctk.CTkProgressBar(master=frame, mode='indeterminate')
progress.pack(pady=10)
progress.pack_forget()  # Hide initially

# Function to run script in thread
def run_script_thread(script_name, folder, msg):
    progress.pack()  # Show loading bar
    progress.start()  # Start spinning

    def task():
        try:
            if folder == "src":
                 path = os.path.join(parent_dir, folder, script_name)
            else:
                path = os.path.join(parent_dir, "src", folder, script_name)
            subprocess.run(["python", path])
        finally:
            app.after(0, progress.stop)
            app.after(0, progress.pack_forget)
            app.after(0, lambda: messagebox.showinfo("Script Finished", msg))

    threading.Thread(target=task).start()

# Function to create labeled run button
def add_script_runner(master, label_text, script_name, folder_name):
    label = ctk.CTkLabel(master=master, text=label_text)
    label.pack(anchor="w", pady=(10, 0))
    
    button = ctk.CTkButton(
        master=master, text="Run", corner_radius=10,
        command=lambda: run_script_thread(script_name, folder_name, f"{label_text} has finished running.")
    )
    button.pack(anchor="w", pady=(0, 10))

# Add buttons
add_script_runner(frame, "Project Setup", "createLogJson.py", "setup")
add_script_runner(frame, "Landsat Processing", "ls8ResampleFull_v2.py", "landsat")
add_script_runner(frame, "Landsat Temporal Processing", "ls8Temporal.py", "landsat")
add_script_runner(frame, "3DEP Processing", "3dep.py", "3dep")
add_script_runner(frame, "Get OSM Data", "getOsmData.py", "osm")
add_script_runner(frame, "Get Extreme Weather Data", "extremeWeatherGetFilterRegion.py", "extreme_weather")
add_script_runner(frame, "Add site data to frontend", "siteCoordsToSFM.py", "src")

app.mainloop()