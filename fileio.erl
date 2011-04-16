%% basic file I/O operations wrapped up nicely

%% NOTE: THE LIST PASSED TO writelines/3 MUST BE A LIST OF STRINGS
%% NOTE: THE RETURN FROM readline/1 WILL BE A LIST OF STRINGS

-module(fileio).
-export([
	 writelines/3,
	 readline/1
	 ]).

writelines(Mode, Filename, [H|T]) ->
    {ok, IOdevice} = file:open(Filename,[Mode]),
    write(IOdevice, [H|T]),
    file:close(IOdevice);
writelines(Mode, Filename, Line) ->
    {ok, IOdevice} = file:open(Filename,[Mode]),
    write(IOdevice, [Line]),
    file:close(IOdevice).

write(IOdevice, [H]) ->
    file:write(IOdevice, "\n"++H);   
write(IOdevice, [H|T]) ->
    file:write(IOdevice, "\n"++H),
    write(IOdevice, T ).

readline(Filename) ->
    case file:read_file(Filename) of 					
        {ok, Contents} ->
	    {ok, string:tokens(erlang:binary_to_list(Contents), "\n")};
	{error, Reason}->
	    {error, Reason}
    end.


    

