flume-ng agent --conf-file flumehdfsconf.properties \
--name agent1 --conf $FLUME_HOME/conf \
-Dflume.root.logger=INFO,console
