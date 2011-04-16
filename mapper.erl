-module(mapper).
-export([
	 mapper/6,
	 mworker/4
	]).


mapper(_, [[], [], _, _], _, _, Task_tracker, _) ->
    io:format("Mapper complete"),
    Task_tracker ! {mapper_complete};


%% mapper organises the mapping operation
%% ensures that only the specified number of mappers are running
mapper(Mapperfunc, [Todo, Processing, Done, Badfile],
       Num_mappers, Num_reducers, Task_tracker, WorkerCount) ->
%%    io:format("~n mapper files : ~p ~n",[{Todo,Processing,Done}]),
    Me = self(),
    if
%%	Todo =:= [] , Processing =:= [] ->
%%	    exit(done);
	    
	WorkerCount < Num_mappers , Todo =/= [] ->
	    [H|_] = Todo,
	    spawn(mapper, mworker, [Mapperfunc, H, Num_reducers, self()]),
	    mapper(Mapperfunc, 
		   [Todo--[H],
		    Processing++[H],
		    Done, 
		    Badfile
		   ],
		   Num_mappers, Num_reducers, Task_tracker, WorkerCount+1);
	true ->
	    ok
    end,	    

    receive
	{mapper_success, Me, Inp_file, Int_files} ->	    
	    io:format("~nMapper: ~p done",[Inp_file]),
	    Task_tracker ! {mapper_result_success, Inp_file, Int_files},
	    mapper(Mapperfunc, 
		   [Todo,
		    Processing--[Inp_file],
		    Done++[Inp_file], 
		    Badfile
		   ],
		   Num_mappers, Num_reducers, Task_tracker, WorkerCount-1);

	{mapper_failure, Me, Inp_file} ->
	    Task_tracker ! {mapper_result_failure, Inp_file},
	    mapper(Mapperfunc, 
		   [Todo,
		    Processing--[Inp_file],
		    Done,
		    Badfile ++[Inp_file]
		   ],
		   Num_mappers, Num_reducers, Task_tracker, WorkerCount-1);
	{die} ->
	    ok		
    end.
    
%% mworker handles a single map job at a time
%% 1. It reads in lines from a specified file
%% 2. Applies the Mapperfun on each line read in
%%        giving a {key,value} pair
%% 3. A phash is applied to the key and list is sorted on the basis on key
%% 4. The new list is split into sublists based on phash
%% 5. The sublists which go to different reducers, are written to different
%%        files, with the reducer number as the prefix
mworker(Mapperfun, Filename, Num_reducers, Mapper_id ) ->    
%%    io:format("~n mworker invoked for file -> ~p",[Filename]),
    case fileio:readline(Filename) of
	{ok, Contents} ->
	    
	    %% result from Mapperfun(X) is a list of {key , value} pairs
	    %% We apply a phash to get the list in the format
	    %% [ [hash, {Key,Value}] | T ]
	    Result =
	      lists:sort(
	      lists:map(fun({Key,Value}) ->
				[erlang:phash(Key,Num_reducers),{Key,Value}]
			end,
			lists:map(fun(X)->
					  Mapperfun(X) 
				  end, 
				  Contents)
		       )
	       ),
	    
	    %% NOTE: THE KEY MUST BE OF TYPE STRING AND VALUE INT OR LIST
	    Lists = [ [ lists:flatten(io_lib:format("~s,~p",[K,V])) ||  
			  [Hash, {K,V}] <- Result, 
					  Hash =:= Seq  ]
		      || Seq  <- lists:seq(1, Num_reducers) ],

	    [Fname | _ ] = string:tokens(Filename,"."),
	    
	    lists:foldl( fun(X, Acc) ->	
				 [Num] = io_lib:format("~p",[Acc]),
				 Out = Fname ++"_" ++Num++ ".int" ,
%%				 io:format("~n~p",[Out]),
				 fileio:writelines(write, Out, X),
				 1+Acc
			 end,
			 0, 
			 Lists),	    
	    
	    %% slightly crappy way of doing this
 	    Intermediates = filelib:wildcard(Fname++"*.int"),
	    %% Send reply to mapper of successful completion
	    Mapper_id ! {mapper_success, Mapper_id, Filename, Intermediates};
	
	%% we are ignoring the reason for now. Till better reason handling is
	%% implemented
	{error, _} ->
	    Mapper_id ! {mapper_failure, Mapper_id, Filename}
    end.

    
