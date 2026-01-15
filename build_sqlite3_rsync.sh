#!/bin/bash

# Install sqlite3_rsync from source
# assumes this 

set -e

# Clone the repository
git clone https://github.com/sqlite/sqlite.git
cd sqlite

# Configure and build
./configure 
make sqlite3_rsync

# moving to bin (creating it first if it does not exist)
mkdir -p ~/bin
mv ./sqlite3_rsync ~/bin
chmod +x ~/bin/sqlite3_rsync

# add bin to bashrc
echo 'export PATH="$HOME/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc

# delete sqlite source files 
rm -rf ~/sqlite

echo "sqlite3_rsync installation complete"