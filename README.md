# onetwotrip


	redis-server --save "" --daemonize yes #start redis on default port without dump.rdb
	git clone https://github.com/h0x91b/onetwotrip.git
	cd onetwotrip
	npm install
	node . --level 3 #log level debug, use 1 for production
	node . --speed 500 #generate messages every 500ms (half second)
	node . --getErrors #get and flush errors

