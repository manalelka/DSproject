#!/bin/bash 
pid=$(ps aux | grep server.py | grep ? | awk '{print $2}') 
kill -INT $pid
