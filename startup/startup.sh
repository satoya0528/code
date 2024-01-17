#!/bin/bash
for file in *.txt; do
    if [[ -f "$file" ]]; then
        echo "Installing requirements from $file..."
        pip install -r "$file" -U
    else
        echo "No .txt files found."
    fi
done