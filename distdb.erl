%%
%% Example of a simple Erlang Distributed Key-Value Database
%% 
%% This was created as an example of Erlang's distributed abilities.
%% A server is started on a node that can then be used to bring up other
%% Erlang nodes running in the mesh.
%%
%% When an item is added into the "database" it is replicated to all other
%% connected nodes. When a new node is added to the mesh, a copy of the current
%% database is sent to it. Items are retrieved from random nodes in the mesh
%% which provides a very simple "load balancing" mechanism.
%%
%% This code is not meant to be function, it is just a demonstration of the
%% distributed features of Erlang.
%%
-module(distdb).
-compile([export_all]).

%set up and register the DB loop
server() ->
	io:format("Starting dbserver on ~w~n", [node()]),
	register(dbserver, spawn(?MODULE, dbLoop, [replicateDB(nodes())])).

bringUpNode(Node) ->
	spawn(Node, ?MODULE, server, []).

%wrapper function to send a reply to a client
reply(Pid, Message) ->
	Pid ! {reply, Message}.

%get a reply and return the message
getReply() ->
	receive
		{reply, Message} -> Message
	end.

%send a message to the dbserver on the target node
sendNodeMessage(Node, Message) ->
	{dbserver,Node} ! {self(), Message}.

%main function loop
dbLoop(DB) ->
	receive
		{Pid, {store, {Key, Value}}} ->
			io:format("~w: Storing new item ~w~n", [node(), {Key,Value}]),
			NewDB = storeItem(DB, Key, Value),
			replicateItem(Key, Value),
			reply(Pid, ok),
			dbLoop(NewDB);
		{Pid, {replicateItem, {Key, Value}}} ->
			io:format("~w: Storing replicated item ~w~n", [node(), {Key,Value}]),
			NewDB = storeItem(DB, Key, Value),
			dbLoop(NewDB);
		{Pid, {retrieve, Key}} ->
			io:format("~w: Fetching item ~w~n", [node(), Key]),
			reply(Pid, fetchItem(DB, Key)),
			dbLoop(DB);
		{Pid, sendDB} ->
			io:format("~w: Sending database to ~w~n", [node(), Pid]),
			reply(Pid, DB),
			dbLoop(DB);
		{Pid, stop} ->
			io:format("~w: Instructed to Terminate ~n", [node()]),
			reply(Pid, stopped)
	end.

%stores an item in the DB and returns a new modified DB structure
storeItem([], Key, Value) -> [{Key, Value}];
storeItem([{Key, _Value} | DBs], Key, Value) -> [{Key, Value} | DBs];
storeItem([D | Ds], Key, Value) -> [D | storeItem(Ds,Key,Value)].

%fetch an item from the DB
fetchItem([], _Key) -> {error, no_key};
fetchItem([{Key, Value} | _DBs], Key) -> Value;
fetchItem([D | Ds], Key) -> fetchItem(Ds, Key).

%performs the replication of data to all of the nodes
replicateItemNodes([], _, _) -> ok;
replicateItemNodes([Node | Nodes], Key, Value) ->
	sendNodeMessage(Node, {replicateItem, {Key, Value}}),
	replicateItemNodes(Nodes, Key, Value).
	
% wrapper function to replicate an item to all the nodes in the mesh
replicateItem(Key, Value) ->
	replicateItemNodes(nodes(), Key, Value).

%fetch the DB from one of the nodes in the mesh
replicateDB([]) -> [];
replicateDB(Nodes) ->
	Node = randomNode(Nodes),
	io:format("~w: replicating DB from ~w~n",[node(),Node]),
	sendNodeMessage(Node, sendDB),
	getReply().

%stop all the nodes in the mesh
stopMeshNodes([]) -> ok;
stopMeshNodes([N | Ns]) ->
	sendNodeMessage(N, stop),
	stopMeshNodes(Ns).

%stop all the clients in the mesh
stopMesh() ->
	stopMeshNodes(nodes()).

%add an item to the datastore
add(Key, Value) ->
	Node = randomNode(nodes()),
	sendNodeMessage(Node, {store, {Key, Value}}),
	getReply().

%fetch a key from the mesh
fetch(Key) ->
	Node = randomNode(nodes()),
	sendNodeMessage(Node, {retrieve, Key}),
	getReply().

%get the size of a list
listSize([]) -> 0;
listSize([_N|Ns]) -> 1 + listSize(Ns).

%return a random node from the list of mesh items
randomNode(NodeList) ->
	lists:nth(random:uniform(listSize(nodes())), nodes()).
