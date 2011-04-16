-module(filesystem).
-export([
	 fs_client/1,
	 fs_server/1,
	 fs_server/2
	 ]).

fs_client(Path) ->
    filelib:wildcard(Path).
   
fs_server(List)->
    [  [X , rpc:call(X, filesystem, fs_client, ["/Data/*.{dat,int}"])] ||
	   X <- List ].

fs_server(List, Path) ->
    [  [X , rpc:call(X, filesystem, fs_client, [Path])] ||
	   X <- List ].
