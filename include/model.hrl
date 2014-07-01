-include("db.hrl").
-export([id/1, new/0, new/1, create/0, create/1, find/1, find_all/0, find_all/1, find_all/2, count/1, exists/1,
  update/1, update/2, update_all/2, delete/1, delete_all/0, delete_all/1, save/1,
  find_or_new/1, find_or_create/1, find_in_batches/2, find_in_batches/3, last/0, last/1]).

-ifdef(TEST).
-export([make/0, make/1, make/2]).
-endif.

id(#?MODULE{id = Id}) -> Id.

new() -> #?MODULE{}.

new(Fields) ->
  set_values(Fields, new()).

set_values({Field, Value}, Record) ->
  setelement(field_index(Field), Record, Value);
set_values([], Record) -> Record;
set_values([Pair | Rest], Record) ->
  set_values(Rest, set_values(Pair, Record)).

save(Record = #?MODULE{id = undefined}) ->
  create(Record);
save(Record) ->
  update(Record).

-ifdef(TEST).
make() -> make(default, []).
make(Kind) when is_atom(Kind) -> make(Kind, []);
make(Fields) when is_list(Fields) -> make(default, Fields).
make(Kind, Fields) ->
  Blueprint = orddict:merge(fun(_, _, V) -> V end,
    orddict:from_list(blueprint:make(?MODULE, Kind)),
    orddict:from_list(Fields)),
  Record = lists:keymap(fun make_blueprint_value/1, 2, Blueprint),
  create(new(Record)).

make_blueprint_value(Tuple) when is_tuple(Tuple) -> Tuple:id();
make_blueprint_value(Fun) when is_function(Fun) -> make_blueprint_value(Fun());
make_blueprint_value(Ref) when is_reference(Ref) -> erlang:ref_to_list(Ref);
make_blueprint_value(Atom) when is_atom(Atom) -> Model = Atom:make(), Model:id();
make_blueprint_value(Value) -> Value.
-endif.

find_or_new(Criteria) ->
  case find(Criteria) of
    undefined -> new(Criteria);
    Record -> Record
  end.

find_or_create(Criteria) ->
  case find(Criteria) of
    undefined -> create(new(Criteria));
    Record -> Record
  end.

create() -> create(#?MODULE{}).

create(Record = #?MODULE{}) ->
  Now = {datetime, calendar:universal_time()},
  RecordToInsert = Record#?MODULE{created_at = Now, updated_at = Now},
  Id = db:insert(insert_query(map_row(RecordToInsert, dump))),
  RecordToInsert#?MODULE{id = Id}.

count(Criteria) ->
  [Count] = db:select_one(select_count_query(Criteria, [])),
  Count.

exists(Id) when is_number(Id) -> exists({id, Id});
exists(#?MODULE{id = Id}) -> exists(Id);
exists(undefined) -> false;
exists(Criteria) -> count(Criteria) > 0.

find(Id) when is_number(Id) ->
  find({id, Id});

find(Criteria) ->
  find_with_cache(Criteria).

-ifdef(CACHE).

find_with_cache(Criteria) ->
  cache:get({?MODULE, Criteria}, fun() ->
    case find_without_cache(Criteria) of
      undefined -> undefined;
      Record -> {{?MODULE, {id, Record#?MODULE.id}}, Record}
    end
  end).

-else.

find_with_cache(Criteria) ->
  find_without_cache(Criteria).

-endif.

find_without_cache(Criteria) ->
  case db:select_one(iolist_to_binary(select_query(Criteria, []))) of
    undefined -> undefined;
    Row -> load_db_row(Row)
  end.

find_all() -> find_all([], []).

find_all(Criteria) -> find_all(Criteria, []).

find_all(Criteria, Options) ->
  Rows = db:select(iolist_to_binary(select_query(Criteria, Options))),
  lists:map(fun load_db_row/1, Rows).

load_db_row(Row) ->
  Record = list_to_tuple([?MODULE | Row]),
  map_row(Record, load).

-ifdef(MAP).
map_row(Record, Op) -> map_row(Record, Op, ?MAP).
map_row(Record, _, []) -> Record;
map_row(Record, Op, [{Field, Mapper} | Rest]) ->
  Index = field_index(Field),
  RawValue = element(Index, Record),
  Value = Mapper:Op(RawValue),
  NewRecord = setelement(Index, Record, Value),
  map_row(NewRecord, Op, Rest).
-else.
map_row(Record, _) -> Record.
-endif.

find_in_batches(Criteria, Fun) ->
  find_in_batches(Criteria, 0, fun(Batch, S) -> Fun(Batch), S end, undefined).

find_in_batches(Criteria, Fun, State) ->
  find_in_batches(Criteria, 0, Fun, State).

find_in_batches(Criteria, From, Fun, State) ->
  case find_all([{id, '>', From} | Criteria], [{order_by, id}, {limit, 1000}]) of
    [] -> ok;
    Batch ->
      NewState = Fun(Batch, State),
      Last = lists:last(Batch),
      find_in_batches(Criteria, Last#?MODULE.id, Fun, NewState)
  end.

last() -> last([]).

last(Criteria) ->
  case find_all(Criteria, [{order_by, id, desc}, {limit, 1}]) of
    [] -> undefined;
    [Row] -> Row
  end.

update(Record = #?MODULE{}) ->
  Now = {datetime, calendar:universal_time()},
  RecordToUpdate = Record#?MODULE{updated_at = Now},
  db:update(update_query(map_row(RecordToUpdate, dump))),
  RecordToUpdate.

update(Fields, Record) ->
  UpdatedRecord = set_values(Fields, Record),
  update(UpdatedRecord).

update_all(FieldsValues, Criteria) ->
  db:update(update_all_query(FieldsValues, Criteria)).

delete(#?MODULE{id = undefined}) ->
  ok;
delete(#?MODULE{id = Id}) ->
  1 = delete_all({id, Id}),
  ok.

delete_all() -> delete_all([]).

delete_all(Criteria) ->
  db:update(delete_query(Criteria)).

insert_query(Record) ->
  insert_fields(<<"INSERT INTO ", ?TABLE_NAME, "(">>, Record, record_info(fields, ?MODULE)).

insert_fields(Query, Record, [id | Rest]) ->
  insert_fields(Query, Record, Rest);
insert_fields(Query, Record, [Field]) ->
  FieldEscBin = list_to_binary(escape_mysql_field(Field)),
  insert_values(<<Query/binary, FieldEscBin/binary, ") VALUES (">>, Record, 3, record_info(size, ?MODULE));
insert_fields(Query, Record, [Field | Rest]) ->
  FieldEscBin = list_to_binary(escape_mysql_field(Field)),
  insert_fields(<<Query/binary, FieldEscBin/binary , ", " >>, Record, Rest).

insert_values(Query, Record, Index, Count) when Index =:= Count ->
  ValueBin = list_to_binary(mysql:encode(element(Index, Record))),
  <<Query/binary, ValueBin/binary, ")">>;
insert_values(Query, Record, Index, Count) ->
  ValueBin = list_to_binary(mysql:encode(element(Index, Record))),
  insert_values(<<Query/binary, ValueBin/binary, ", ">>, Record, Index + 1, Count).

select_query(Criteria, Options) ->
  ["SELECT " | select_fields(Criteria, Options, record_info(fields, ?MODULE))].

select_count_query(Criteria, Options) ->
  ["SELECT COUNT(*) FROM ", ?TABLE_NAME | select_criteria(Criteria, Options)].

select_fields(Criteria, Options, [Field]) ->
  [ escape_mysql_field(Field) , " FROM ", ?TABLE_NAME | select_criteria(Criteria, Options)];
select_fields(Criteria, Options, [Field | Rest]) ->
  [ escape_mysql_field(Field) , ", " | select_fields(Criteria, Options, Rest)].

select_criteria([], Options) -> select_options(Options);
select_criteria(Criteria, Options) ->
  [" WHERE ", select_criteria(Criteria) | select_options(Options)].

select_criteria({Field, in, Values}) ->
  [escape_mysql_field(Field), " IN (", value_list(Values), ")"];

select_criteria({C1, 'or', C2}) ->
  ["(", select_criteria(C1), " OR ", select_criteria(C2), ")"];

select_criteria({Field, '<', Value}) ->
  [escape_mysql_field(Field), " < ", mysql:encode(Value)];

select_criteria({Field, '<=', Value}) ->
  [escape_mysql_field(Field), " <= ", mysql:encode(Value)];

select_criteria({Field, '>', Value}) ->
  [escape_mysql_field(Field), " > ", mysql:encode(Value)];

select_criteria({Field, '>=', Value}) ->
  [escape_mysql_field(Field), " >= ", mysql:encode(Value)];

select_criteria({Field, null}) ->
  [escape_mysql_field(Field), " IS NULL"];

select_criteria({Field, Value}) ->
  [escape_mysql_field(Field), " = ", mysql:encode(Value)];

select_criteria([Filter]) ->
  select_criteria(Filter);

select_criteria([Filter | Rest]) ->
  [select_criteria(Filter), " AND " | select_criteria(Rest)].

select_options([]) -> [];
select_options([{order_by, Field} | Rest]) ->
  [" ORDER BY ", escape_mysql_field(Field) | select_options(Rest)];
select_options([{order_by, Field, desc} | Rest]) ->
  [" ORDER BY ", escape_mysql_field(Field), " DESC" | select_options(Rest)];
select_options([{limit, Limit} | Rest]) ->
  [" LIMIT ", integer_to_list(Limit) | select_options(Rest)].

update_query(Record) ->
  ["UPDATE ", ?TABLE_NAME, " SET " | update_fields(Record, 2, record_info(fields, ?MODULE))].

update_fields(Record, Index, [id | Rest]) ->
  update_fields(Record, Index + 1, Rest);

update_fields(Record, Index, [Field]) ->
  [ escape_mysql_field(Field) , " = ", mysql:encode(element(Index, Record)), " WHERE id = ", mysql:encode(Record#?MODULE.id)];

update_fields(Record, Index, [Field | Rest]) ->
  [ escape_mysql_field(Field) , " = ", mysql:encode(element(Index, Record)), ", " | update_fields(Record, Index + 1, Rest)].

update_all_query(FieldsValues, Criteria) ->
  ["UPDATE ", ?TABLE_NAME, " SET " | update_all_fields(FieldsValues, Criteria)].

update_all_fields([{Field, Value}], Criteria) ->
  [escape_mysql_field(Field), " = ", mysql:encode(Value) | select_criteria(Criteria, [])];

update_all_fields([{Field, Value} | FieldsValues], Criteria) ->
  [escape_mysql_field(Field), " = ", mysql:encode(Value), ", " | update_all_fields(FieldsValues, Criteria)].

delete_query(Criteria) ->
  ["DELETE FROM ", ?TABLE_NAME | select_criteria(Criteria, [])].

value_list([]) -> [];
value_list([Value]) ->
  [mysql:encode(Value)];
value_list([Value | Rest]) ->
  [mysql:encode(Value), ", " | value_list(Rest)].

field_index(Field) ->
  field_index(Field, record_info(fields, ?MODULE), 2).

field_index(Field, [Field | _], N) -> N;
field_index(Field, [_ | Rest], N) -> field_index(Field, Rest, N + 1).

escape_mysql_field(Field) -> [$`, atom_to_list(Field), $`].
