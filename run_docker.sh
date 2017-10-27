docker build -t torch .
docker run -v C:\DevP\fb\data:/root/torch-rnn/data -it torch bash
th train.lua -input_h5 data/comments.h5 -input_json data/comments.json -gpu -1
th train.lua -input_h5 data/comments.h5 -input_json data/comments.json -gpu -1 -batch_size 80
th train.lua -input_h5 data/comments.h5 -input_json data/comments.json -gpu -1 -batch_size 80 -rnn_size 128 -num_layers 2 -dropout 0 -checkpoint_every 100 # -speed_benchmark 1 0.7s
# 00:37 am

# Total vocabulary size: 958
# Total tokens in file: 117202911
#   Training size: 93762329
#   Val size: 11720291
#   Test size: 11720291