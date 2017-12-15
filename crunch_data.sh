cd /home/h2ndxygv/xkcd_bot
source venv/bin/activate

if pgrep -x "python" > /dev/null
then
    echo "Listener already running"
else
    echo "Starting listener"
    python main.py &
    disown
fi

python data_cruncher.py

mv ./data ../xkcdredditstats.com/data
mv ./rawdata.zip ../xkcdredditstats/