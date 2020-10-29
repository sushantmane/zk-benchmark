#rsync -a --exclude='venv/' .  013736658@cs-reed-01:/srv/home/013736658/test/
#rsync -a --exclude='venv' 013736658@cs-reed-01:/srv/home/013736658/test/ .
#rsync -a --exclude={'env','zookeeper-3.7.0-SNAPSHOT-fatjar.jar'} 013736658@cs-reed-01:/srv/home/013736658/zk-bench-runner/*