-module(pingserver).
-export([
	 rpc_handler/2,
	 start/1,
	 ping_server/1,
	 ping/1,
	 pong/0
	 ]).


ping(Node) ->    
    {N, Status} = rpc:call(Node, pingserver, pong, []),
    if 
	Status =:= up ->
	    {Status, N};
	Status =:= nodedown ->
	    {down, Node}
    end.
      
pong() ->
    {node(), up}.

ping_server(List) ->
    Result = [ ping(Node) || Node <- List ],
    [[ {Status, Node} || {Status, Node} <- Result ,
			Status =:= up ],
     [ {Status, Node} || {Status, Node} <- Result ,
			Status =:= down ]
    ].


start(List) ->
    Pid = register(ping_server_pid, spawn(mapreduce, ping_server, [List])),
    io:format("~nstart: ping_server_pid registered to: ~p ~n",[Pid]).
    
   
server(List) ->
    io:format("ping_server: Task_tracker status-list: ~p~n",[List]),
    receive
	{up, T_tracker, _, _} ->
	    io:format("~nping_server: received message {up, ~p}",[T_tracker]),
	    ping_server( update(List, {up,T_tracker}) );
	{down, T_tracker, _} ->
	    io:format(" ping_server: received message {down, ~p}~n",[T_tracker]),
	    ping_server( update(List, {down,T_tracker}) );
	{die } ->
	    io:format(" Ping server exiting... ~n")
    after 10000 ->			
	    lists:map( fun(X) ->
			       spawn(mapreduce, rpc_ping, [X]) end,
		       List ),
	    ping_server(List)
    end.
	    
rpc1_ping(T_tracker) ->
    io:format("rpc_ping: Pinging T_tracker -> ~p~n",[T_tracker]), 
    {_,Timestamp1} = erlang:localtime(),
    Result = rpc:call(T_tracker, mapreduce, rpc_pong, [node(), self()]),
    io:format("rpc_ping: Result from calling rpc_pong : ~p~n",[Result]),
    case Result of
	{ping, _} ->
	    receive
		{ping, T_tracker} ->
		    {_, Timestamp2} = erlang:localtime(),
		    io:format("Received pong from ~p at ~p, ping sent at ~p~n",
		              [T_tracker, Timestamp2, Timestamp1]),
		    ping_server_pid ! {up, T_tracker, Timestamp2, Timestamp1}
	    after 10000 ->		    
		    ping_server_pid ! {down, T_tracker, Timestamp1}
	    end;
	{badrpc, Reason} ->
	    io:format("rpc_ping: T_tracker at ~p is down due to ~p~n",
		      [T_tracker, Reason]),
	    ping_server_pid ! {down, T_tracker, Timestamp1};
	true ->
	    ping_server_pid ! {nodedown, T_tracker}
    end.

				       
rpc1_pong(Origin, Server_pid) ->
    io:format("~nReceived ping from ~p ",[Origin]),
    rpc:call(Origin, mapreduce, rpc_handler, 
	     [Server_pid, {ping, node()}]).

rpc_handler(Pid, Request) ->
    Pid ! Request .
    
    
