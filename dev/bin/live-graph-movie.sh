#!/bin/bash

set -eu

usage() {
    cat <<EOF

USAGE: $0 <SUITE>

Convert frame-*.dot files in the suite share directory (as generated by the
gcylc graph view if the corresponding menu option is selecte) to a frames.webm
movie, which can be played natively in the Firefox browser.

Safe to re-run with different frame size and rate.

# Defaults:
\$FRAME_SIZE="1200x900" # pixels
\$FRAME_RATE=2 # frames per second

EOF
}

# Requirements:
#  * dot (graphviz)
#  * convert (imagemagick
#  * png2yuv (mjpegtools)
#  * vpxenc (vpx-tools)
# Documentation:
#  * http://wiki.webmproject.org/howtos/convert-png-frames-to-webm-video
#  * http://www.webmproject.org/docs/encoder-parameters/

if [[ $1 == "-h" || $1 == "--help" || $1 == "help" ]]; then
    usage
    exit 0
fi

SUITE=$1
if ! cylc db pr --fail $SUITE >/dev/null; then
    usage
    exit 1
fi

FRAME_SIZE=${FRAME_SIZE:-"1200x900"} # pixels
FRAME_RATE=${FRAME_RATE:-2} # frames per second

# Move to the suite share directory.
cd $( cylc get-global-config --print-run-dir )/$SUITE/share

N_FILES=$(ls frame-*.dot 2> /dev/null | wc -l)
if [[ $N_FILES -eq 0 ]]; then
    echo "[ERROR] No frame-*.dot files found in ${PWD}."
    exit 1
fi
N_FILES=$(( N_FILES - 1 )) # starts from 0
CHARS=$(echo $N_FILES | wc -m)
DIGITS=$(( CHARS - 1 )) # ignore newline char
echo
echo "[INFO] Zero-padding frame numbers to $DIGITS digits."
FORMAT="%0${DIGITS}d"
PADDED=false
# Renumber to allow for manual removal and addition of frames.
COUNT=0
for DOT_ORIG in $(ls -v frame-*.dot); do 
    TMP=${DOT_ORIG%.dot}
    N_OLD=${TMP#frame-}
    printf -v N_NEW "$FORMAT" $(( 10#$COUNT ))
    DOT_NEW=frame-${N_NEW}.dot
    if [[ "$N_NEW" != "$N_OLD" ]]; then
        PADDED=true
        echo -ne \\r
        CMD="mv $DOT_ORIG $DOT_NEW"
        echo -n $DOT_NEW
        $CMD
        sleep 0.05
    fi
    COUNT=$(( COUNT + 1 ))
done
$PADDED && echo

echo
echo "[INFO] Generating .png images."
for DOT in $(ls -v frame-*.dot); do
    PNG=${DOT%dot}png
    echo -ne \\r
    echo -n $PNG
    dot -Tpng -Gsize=9,9\! -o $PNG $DOT -Nfontname=Courier\ 10\ Pitch
done
echo

echo
echo "[INFO] Generating white-padded $FRAME_SIZE .png frames."
for PNG in $(ls -v frame-*.png); do
    NEW_PNG=movie-$PNG
    echo -ne \\r
    echo -n $NEW_PNG
    convert -quality 100 -resize $FRAME_SIZE -background white -gravity West \
        -extent $FRAME_SIZE $PNG $NEW_PNG
done
echo

# Alternative movie format:
#echo "GENERATING mp4 movie"
#ffmpeg -sameq -r 2 -f image2 -i movie-frame-${FORMAT}.png vid.mp4

echo
echo "[INFO] Generating .webm movie ($FRAME_RATE frames/second)."
# convert PNG frames to YUV video
png2yuv -I p -f $FRAME_RATE -b 0 -j movie-frame-${FORMAT}.png > frames.yuv
# convert YUV video to webm format:
vpxenc --good --cpu-used=0 --auto-alt-ref=1 --lag-in-frames=16 \
    --end-usage=vbr --passes=2 --threads=2 --target-bitrate=3000 -o \
    frames.webm frames.yuv

# Clean up intermediate files.
#rm *.png *.yuv

echo
echo "[INFO] Done."
echo "$PWD/frames.webm"
