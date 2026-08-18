defmodule SpectrumPhx.ZkTest do
  @moduledoc """
  Wire-protocol and cluster-state tests for the ZooKeeper client.

  The encoding tests assert exact bytes: this is a hand-rolled implementation of someone
  else's wire format, so "it round-trips through our own decoder" is not evidence that
  ZooKeeper would accept it. The byte literals here were taken from the Python reference
  client, which is validated against ZooKeeper 3.9.2.

  The socket-level tests run against a fake server implemented at the bottom of this
  file. It speaks just enough of the protocol to exercise framing, the handshake, xid
  correlation, watch-notification skipping, and server-side error codes -- none of which
  can be reached by testing the pure functions alone.
  """
  use ExUnit.Case, async: true

  alias SpectrumPhx.Zk.Client
  alias SpectrumPhx.Zk.State

  # A Stat struct: czxid, mzxid, ctime, mtime, version, cversion, aversion,
  # ephemeralOwner, dataLength, numChildren, pzxid.
  @stat :binary.copy(<<0>>, 68)

  describe "string and buffer encoding" do
    test "a string is an int32 length followed by its UTF-8 bytes" do
      assert Client.encode_string("abc") == <<0, 0, 0, 3, ?a, ?b, ?c>>
      assert Client.encode_string("") == <<0, 0, 0, 0>>
    end

    test "length is in bytes, not codepoints" do
      assert Client.encode_string("héllo") == <<0, 0, 0, 6>> <> "héllo"
    end

    test "nil encodes as length -1" do
      assert Client.encode_string(nil) == <<255, 255, 255, 255>>
      assert Client.encode_buffer(nil) == <<255, 255, 255, 255>>
    end

    test "a buffer is framed like a string" do
      assert Client.encode_buffer(<<0, 1, 2>>) == <<0, 0, 0, 3, 0, 1, 2>>
      assert Client.encode_buffer(<<>>) == <<0, 0, 0, 0>>
    end

    test "decoding returns the value and the offset just past it" do
      assert Client.decode_string(<<0, 0, 0, 3>> <> "abc", 0) == {"abc", 7}
      assert Client.decode_string(<<0, 0, 0, 0>>, 0) == {"", 4}
    end

    test "decoding honours a starting offset" do
      binary = <<9, 9, 9, 9>> <> <<0, 0, 0, 2>> <> "hi" <> "trailing"
      assert Client.decode_string(binary, 4) == {"hi", 10}
    end

    test "a length of -1 decodes to nil and consumes only the length" do
      assert Client.decode_string(<<255, 255, 255, 255, 7, 7>>, 0) == {nil, 4}
      assert Client.decode_buffer(<<255, 255, 255, 255>>, 0) == {nil, 4}
    end

    test "consecutive strings decode by chaining offsets" do
      binary = Client.encode_string("one") <> Client.encode_string("two")
      {first, offset} = Client.decode_string(binary, 0)
      {second, offset} = Client.decode_string(binary, offset)

      assert first == "one"
      assert second == "two"
      assert offset == byte_size(binary)
    end

    test "a truncated string raises rather than returning a partial value" do
      assert_raise MatchError, fn -> Client.decode_string(<<0, 0, 0, 8, ?a>>, 0) end
    end
  end

  describe "request framing" do
    test "a request body is xid, opcode, then payload" do
      assert Client.encode_request(7, 4, "xy") == <<0, 0, 0, 7, 0, 0, 0, 4, ?x, ?y>>
    end

    test "xids are signed, so the ping xid is -2" do
      assert Client.encode_request(-2, 11, <<>>) == <<255, 255, 255, 254, 0, 0, 0, 11>>
    end

    test "a frame is prefixed with its int32 body length" do
      assert Client.frame(<<1, 2, 3>>) == <<0, 0, 0, 3, 1, 2, 3>>
      assert Client.frame(<<>>) == <<0, 0, 0, 0>>
    end

    test "opcodes match the protocol" do
      assert Client.opcodes() == %{
               create: 1,
               delete: 2,
               exists: 3,
               get_data: 4,
               set_data: 5,
               get_children: 8,
               ping: 11,
               close: -11
             }
    end
  end

  describe "reply parsing" do
    test "a reply header is xid, zxid, then the error code" do
      frame = <<0, 0, 0, 5>> <> <<0, 0, 0, 0, 0, 0, 0, 9>> <> <<0, 0, 0, 0>>
      assert Client.decode_reply_header(frame) == {:ok, 5, 9, 0}
    end

    test "the error code is signed" do
      frame = <<0, 0, 0, 5>> <> <<0::64>> <> <<255, 255, 255, 155>>
      assert {:ok, 5, 0, -101} = Client.decode_reply_header(frame)
    end

    test "zxid is a 64-bit value" do
      frame = <<0, 0, 0, 1>> <> <<0, 0, 0, 1, 0, 0, 0, 0>> <> <<0::32>>
      assert {:ok, 1, 4_294_967_296, 0} = Client.decode_reply_header(frame)
    end

    test "watch notifications carry xid -1" do
      frame = <<255, 255, 255, 255>> <> <<0::64>> <> <<0::32>>
      assert {:ok, -1, 0, 0} = Client.decode_reply_header(frame)
    end

    test "a short frame is rejected rather than matched partially" do
      assert Client.decode_reply_header(<<0, 0, 0, 5>>) == :error
      assert Client.decode_reply_header(<<>>) == :error
    end
  end

  describe "error codes" do
    test "maps the codes the client acts on" do
      assert Client.error_reason(0) == :ok
      assert Client.error_reason(-101) == :no_node
      assert Client.error_reason(-110) == :node_exists
      assert Client.error_reason(-111) == :not_empty
      assert Client.error_reason(-112) == :session_expired
    end

    test "an unmapped code keeps its number rather than becoming a generic failure" do
      assert Client.error_reason(-4) == {:zk_error, -4}
      assert Client.error_reason(-102) == {:zk_error, -102}
    end
  end

  describe "the connect handshake" do
    test "a ConnectRequest is protocolVersion, lastZxidSeen, timeout, sessionId, passwd, readOnly" do
      passwd = :binary.copy(<<0>>, 16)

      assert Client.encode_connect_request(15_000, 0, passwd) ==
               <<0, 0, 0, 0>> <>
                 <<0::64>> <>
                 <<0, 0, 58, 152>> <>
                 <<0::64>> <>
                 <<0, 0, 0, 16>> <> passwd <> <<0>>
    end

    test "a resumed session sends back the id and password it was given" do
      passwd = :binary.copy(<<7>>, 16)
      encoded = Client.encode_connect_request(6_000, 0x1234_5678, passwd)

      assert <<0::32, 0::64, 6_000::32, 0x1234_5678::64, rest::binary>> = encoded
      assert rest == <<0, 0, 0, 16>> <> passwd <> <<0>>
    end

    test "a ConnectResponse yields the negotiated timeout, session id, and password" do
      passwd = :binary.copy(<<3>>, 16)
      frame = <<0::32, 6_000::32, 0x1234::64>> <> Client.encode_buffer(passwd)

      assert {:ok, response} = Client.decode_connect_response(frame)
      assert response.negotiated_timeout_ms == 6_000
      assert response.session_id == 0x1234
      assert response.passwd == passwd
    end

    test "a session id of zero is reported as-is, so the caller can treat it as expiry" do
      frame = <<0::32, 6_000::32, 0::64>> <> Client.encode_buffer(:binary.copy(<<0>>, 16))
      assert {:ok, %{session_id: 0}} = Client.decode_connect_response(frame)
    end

    test "trailing bytes (3.5+ readOnly) are ignored" do
      passwd = :binary.copy(<<1>>, 16)
      frame = <<0::32, 4_000::32, 99::64>> <> Client.encode_buffer(passwd) <> <<0>>

      assert {:ok, %{negotiated_timeout_ms: 4_000, session_id: 99}} =
               Client.decode_connect_response(frame)
    end

    test "a truncated ConnectResponse is an error, not a crash" do
      assert Client.decode_connect_response(<<0, 0, 0, 0>>) ==
               {:error, :malformed_connect_response}

      assert Client.decode_connect_response(<<>>) == {:error, :malformed_connect_response}
    end
  end

  describe "operation payloads" do
    test "create carries the path, data, an open ACL, and the create mode" do
      assert Client.encode_create("/x", "d", false) ==
               Client.encode_string("/x") <>
                 Client.encode_buffer("d") <>
                 <<0, 0, 0, 1>> <>
                 <<0, 0, 0, 31>> <>
                 Client.encode_string("world") <>
                 Client.encode_string("anyone") <>
                 <<0, 0, 0, 0>>
    end

    test "the ephemeral flag is the difference between surviving a crash and not" do
      persistent = Client.encode_create("/x", "", false)
      ephemeral = Client.encode_create("/x", "", true)

      assert binary_part(persistent, byte_size(persistent), -4) == <<0, 0, 0, 0>>
      assert binary_part(ephemeral, byte_size(ephemeral), -4) == <<0, 0, 0, 1>>
    end

    test "the ACL is world:anyone with all permissions" do
      assert Client.encode_acl() ==
               <<0, 0, 0, 1, 0, 0, 0, 0x1F>> <>
                 Client.encode_string("world") <> Client.encode_string("anyone")
    end

    test "exists, getData and getChildren send a path and a one-byte watch flag" do
      assert Client.encode_path_watch("/a", false) == Client.encode_string("/a") <> <<0>>
      assert Client.encode_path_watch("/a", true) == Client.encode_string("/a") <> <<1>>
    end

    test "setData carries a version, where -1 means unconditional" do
      assert Client.encode_set_data("/a", "v", -1) ==
               Client.encode_string("/a") <> Client.encode_buffer("v") <> <<255, 255, 255, 255>>

      assert Client.encode_set_data("/a", "v", 3) ==
               Client.encode_string("/a") <> Client.encode_buffer("v") <> <<0, 0, 0, 3>>
    end

    test "delete carries a path and a version" do
      assert Client.encode_delete("/a", -1) ==
               Client.encode_string("/a") <> <<255, 255, 255, 255>>
    end
  end

  describe "operation responses" do
    test "create returns the path the server actually created" do
      frame = header(1, 0) <> Client.encode_string("/helios/nodes/10.0.0.1")
      assert Client.decode_create_response(frame) == "/helios/nodes/10.0.0.1"
    end

    test "getData returns the data and ignores the Stat that follows it" do
      frame = header(2, 0) <> Client.encode_buffer(~s({"ip":"10.0.0.1"})) <> @stat
      assert Client.decode_get_data_response(frame) == ~s({"ip":"10.0.0.1"})
    end

    test "a null data buffer reads as empty rather than nil" do
      frame = header(2, 0) <> Client.encode_buffer(nil) <> @stat
      assert Client.decode_get_data_response(frame) == ""
    end

    test "getChildren returns a count followed by that many strings" do
      frame =
        header(3, 0) <>
          <<0, 0, 0, 2>> <>
          Client.encode_string("10.10.102.41") <> Client.encode_string("10.10.102.42")

      assert Client.decode_children_response(frame) == ["10.10.102.41", "10.10.102.42"]
    end

    test "an empty tree decodes to an empty list" do
      assert Client.decode_children_response(header(3, 0) <> <<0, 0, 0, 0>>) == []
    end

    test "children come back in the order the server sent them" do
      names = ~w(c a b)
      body = Enum.map_join(names, &Client.encode_string/1)
      frame = header(3, 0) <> <<length(names)::32>> <> body

      assert Client.decode_children_response(frame) == names
    end
  end

  describe "starting without ZooKeeper" do
    test "start_link succeeds and the client retries in the background" do
      client = start_client(hosts: ["127.0.0.1"], port: closed_port())

      refute Client.connected?(client)
      assert Process.alive?(client)
      assert Client.connected_host(client) == nil
    end

    test "operations report not connected instead of raising" do
      client = start_client(hosts: ["127.0.0.1"], port: closed_port())

      assert Client.get(client, "/helios/nodes") == {:error, :not_connected}
      assert Client.get_children(client, "/helios/nodes") == {:error, :not_connected}
      assert Client.exists(client, "/x") == {:error, :not_connected}
      assert Client.set(client, "/x", "v") == {:error, :not_connected}
      assert Client.delete(client, "/x") == {:error, :not_connected}
      assert Client.create(client, "/x") == {:error, :not_connected}

      assert Process.alive?(client)
    end

    test "an unreachable host does not stop the client from being supervised" do
      client = start_client(hosts: ["127.0.0.1", "127.0.0.1"], port: closed_port())

      # Give the retry loop room to fail through both hosts and back around.
      refute Client.connected?(client)
      assert Process.alive?(client)
    end
  end

  describe "against a server" do
    test "establishes a session and answers reads" do
      port =
        start_fake_zk(%{
          {8, "/helios/nodes"} => {:ok, <<0, 0, 0, 1>> <> Client.encode_string("10.10.102.41")},
          {4, "/cluster_state"} => {:ok, Client.encode_buffer("started") <> @stat}
        })

      client = start_client(port: port)

      assert Client.connected?(client)
      assert Client.connected_host(client) == "127.0.0.1"
      assert Client.get_children(client, "/helios/nodes") == {:ok, ["10.10.102.41"]}
      assert Client.get(client, "/cluster_state") == {:ok, "started"}
    end

    test "skips watch notifications that arrive before a reply" do
      # The fake server emits an xid -1 notification ahead of every reply, so any
      # successful read here proves the client did not mistake one for its answer.
      port = start_fake_zk(%{{4, "/a"} => {:ok, Client.encode_buffer("value") <> @stat}})
      client = start_client(port: port)

      assert Client.get(client, "/a") == {:ok, "value"}
      assert Client.get(client, "/a") == {:ok, "value"}
    end

    test "correlates replies by an xid that advances per request" do
      port = start_fake_zk(%{{4, "/a"} => {:ok, Client.encode_buffer("v") <> @stat}}, self())
      client = start_client(port: port)

      assert {:ok, "v"} = Client.get(client, "/a")
      assert_receive {:zk_request, first, 4, "/a"}, 1_000

      assert {:ok, "v"} = Client.get(client, "/a")
      assert_receive {:zk_request, second, 4, "/a"}, 1_000

      assert second == first + 1
    end

    test "pings on the session keepalive xid" do
      port = start_fake_zk(%{{4, "/a"} => {:ok, Client.encode_buffer("v") <> @stat}}, self())
      client = start_client(port: port)

      send(client, :ping)
      assert_receive {:zk_request, -2, 11, _path}, 1_000

      # The session survived the ping and the reply was not left in the socket for the
      # next request to pick up.
      assert Client.get(client, "/a") == {:ok, "v"}
    end

    test "maps server-side error codes" do
      port =
        start_fake_zk(%{
          {3, "/present"} => {:ok, @stat},
          {1, "/taken"} => {:error, -110},
          {2, "/gone"} => {:error, -101}
        })

      client = start_client(port: port)

      assert Client.exists(client, "/present") == {:ok, true}
      # The fake answers anything unmapped with NoNode.
      assert Client.exists(client, "/absent") == {:ok, false}
      assert Client.get(client, "/absent") == {:error, :no_node}
      assert Client.create(client, "/taken", "d") == {:error, :node_exists}

      # Deleting something that is already absent is the desired end state.
      assert Client.delete(client, "/gone") == :ok
    end

    test "ensure_path creates each missing level and tolerates the ones that exist" do
      port =
        start_fake_zk(
          %{
            {1, "/helios"} => {:error, -110},
            {1, "/helios/nodes"} => {:ok, Client.encode_string("/helios/nodes")}
          },
          self()
        )

      client = start_client(port: port)

      assert Client.ensure_path(client, "/helios/nodes") == :ok
      assert_receive {:zk_request, _, 1, "/helios"}, 1_000
      assert_receive {:zk_request, _, 1, "/helios/nodes"}, 1_000
    end

    test "upsert_ephemeral falls back to a write when this session already owns the node" do
      path = "/helios/nodes/10.0.0.1"

      port =
        start_fake_zk(
          %{
            {1, "/helios"} => {:error, -110},
            {1, "/helios/nodes"} => {:error, -110},
            {1, path} => {:error, -110},
            {5, path} => {:ok, @stat}
          },
          self()
        )

      client = start_client(port: port)

      assert Client.upsert_ephemeral(client, path, "{}") == :ok
      assert_receive {:zk_request, _, 5, ^path}, 1_000
    end

    test "reconnects after the connection is lost" do
      port = start_fake_zk(%{{4, "/a"} => {:ok, Client.encode_buffer("v") <> @stat}}, self())
      client = start_client(port: port)

      assert Client.get(client, "/a") == {:ok, "v"}
      assert_receive {:zk_connected, server}, 1_000

      # Drop the server side of the socket the way a restarting ensemble would.
      send(server, :hang_up)
      assert_receive :zk_hung_up, 1_000

      # The next request observes the closed socket and drops the connection; the
      # background retry then re-establishes it against the still-listening port.
      assert {:error, _reason} = Client.get(client, "/a")
      assert_receive {:zk_connected, _reconnected}, 5_000
      assert eventually(fn -> Client.get(client, "/a") == {:ok, "v"} end)
    end
  end

  describe "read_cluster_state/1" do
    test "returns the published documents keyed by IP" do
      documents = %{
        "10.10.102.41" => %{
          "ip" => "10.10.102.41",
          "hostname" => "helios-01",
          "zk_leader" => true,
          "maintenance_status" => "NORMAL",
          "disks" => 4,
          "ts" => System.system_time(:second),
          "build" => "2026.08.01",
          "services" => %{
            "ZooKeeper" => %{"status" => "UP", "pids" => [1234], "restarts" => 0}
          }
        },
        "10.10.102.42" => %{
          "ip" => "10.10.102.42",
          "hostname" => "helios-02",
          "services" => %{
            "Spark" => %{"status" => "FLAPPING", "pids" => [], "restarts" => 7}
          }
        }
      }

      client = start_client(port: start_fake_zk(cluster_responses(documents, "started")))

      assert {:ok, state} = State.read_cluster_state(client)
      assert state.desired == "started"
      assert state.via == "127.0.0.1"
      assert Map.keys(state.nodes) |> Enum.sort() == ["10.10.102.41", "10.10.102.42"]
      assert state.nodes["10.10.102.41"]["hostname"] == "helios-01"
      assert state.nodes["10.10.102.41"]["zk_leader"] == true
      assert state.nodes["10.10.102.41"]["services"]["ZooKeeper"]["status"] == "UP"
      assert state.nodes["10.10.102.42"]["services"]["Spark"]["restarts"] == 7
    end

    test "an absent /helios/nodes is a successful read with no nodes, not an error" do
      # This is the distinction `cluster status` depends on: ZooKeeper answered, and the
      # answer is that nothing has registered.
      port =
        start_fake_zk(%{{4, "/cluster_state"} => {:ok, Client.encode_buffer("stopped") <> @stat}})

      client = start_client(port: port)

      assert {:ok, %{nodes: %{}, desired: "stopped"}} = State.read_cluster_state(client)
    end

    test "an unset /cluster_state reads as nil rather than failing the whole read" do
      port = start_fake_zk(%{{8, "/helios/nodes"} => {:ok, <<0, 0, 0, 0>>}})
      client = start_client(port: port)

      assert {:ok, %{nodes: %{}, desired: nil}} = State.read_cluster_state(client)
    end

    test "an empty /cluster_state reads as nil" do
      port =
        start_fake_zk(%{
          {8, "/helios/nodes"} => {:ok, <<0, 0, 0, 0>>},
          {4, "/cluster_state"} => {:ok, Client.encode_buffer("  \n") <> @stat}
        })

      assert {:ok, %{desired: nil}} = State.read_cluster_state(start_client(port: port))
    end

    test "a node publishing invalid JSON is skipped, not fatal" do
      good = %{"ip" => "10.10.102.41", "hostname" => "helios-01", "ts" => 1}

      responses =
        %{
          {8, "/helios/nodes"} =>
            {:ok,
             <<0, 0, 0, 2>> <>
               Client.encode_string("10.10.102.41") <> Client.encode_string("10.10.102.42")},
          {4, "/helios/nodes/10.10.102.41"} =>
            {:ok, Client.encode_buffer(Jason.encode!(good)) <> @stat},
          {4, "/helios/nodes/10.10.102.42"} => {:ok, Client.encode_buffer("{not json") <> @stat},
          {4, "/cluster_state"} => {:ok, Client.encode_buffer("started") <> @stat}
        }

      client = start_client(port: start_fake_zk(responses))

      assert {:ok, state} = State.read_cluster_state(client)
      assert Map.keys(state.nodes) == ["10.10.102.41"]
    end

    test "a node whose document disappears between listing and reading is skipped" do
      port =
        start_fake_zk(%{
          {8, "/helios/nodes"} => {:ok, <<0, 0, 0, 1>> <> Client.encode_string("10.10.102.41")},
          # No entry for the getData, so the fake answers NoNode.
          {4, "/cluster_state"} => {:ok, Client.encode_buffer("started") <> @stat}
        })

      assert {:ok, %{nodes: %{}, desired: "started"}} =
               State.read_cluster_state(start_client(port: port))
    end

    test "an unreachable ZooKeeper is an error, and is distinguishable from an empty cluster" do
      client = start_client(hosts: ["127.0.0.1"], port: closed_port())

      assert State.read_cluster_state(client) == {:error, :not_connected}
    end

    test "a client process that is not running is an error, not a crash" do
      assert State.read_cluster_state(:no_such_zk_client) == {:error, :not_connected}
    end

    test "defaults to the named client, and errors when it cannot reach ZooKeeper" do
      # `read_cluster_state/0` is the arity `cluster status` calls. The named client IS
      # supervised by the application, but there is no ZooKeeper in the test environment,
      # so the default arity must still surface a clean {:error, :not_connected} rather
      # than raising -- that is what lets the CLI fall back to the direct probe.
      assert Process.whereis(Client)
      assert State.read_cluster_state() == {:error, :not_connected}
    end
  end

  describe "node_stale?/1" do
    test "a document published just now is fresh" do
      refute State.node_stale?(%{"ts" => System.system_time(:second)})
    end

    test "a document older than the threshold is stale" do
      assert State.node_stale?(%{"ts" => System.system_time(:second) - 31})
      assert State.node_stale?(%{"ts" => System.system_time(:second) - 3_600})
    end

    test "the boundary is exclusive, matching the reference implementation" do
      refute State.node_stale?(%{"ts" => System.system_time(:second) - 30})
    end

    test "a document with no timestamp is stale" do
      assert State.node_stale?(%{"ip" => "10.0.0.1"})
      assert State.node_stale?(%{"ts" => nil})
      assert State.node_stale?(%{})
    end

    test "a timestamp published as a string or float is still read" do
      now = System.system_time(:second)

      refute State.node_stale?(%{"ts" => Integer.to_string(now)})
      refute State.node_stale?(%{"ts" => now * 1.0})
      assert State.node_stale?(%{"ts" => "not a number"})
    end

    test "anything that is not a document is stale" do
      assert State.node_stale?(nil)
      assert State.node_stale?("")
    end

    test "the age is reported in seconds" do
      assert State.node_age_seconds(%{"ts" => System.system_time(:second) - 12}) == 12
    end
  end

  describe "constants" do
    test "match the paths spark-daemon publishes to" do
      assert State.nodes_path() == "/helios/nodes"
      assert State.cluster_state_path() == "/cluster_state"
      assert State.stale_after_seconds() == 30
    end

    test "service order starts with ZooKeeper and ends with the gated Urbosa" do
      order = State.service_display_order()

      assert List.first(order) == "ZooKeeper"
      assert List.last(order) == "Urbosa"
      assert "Spark" in order
      assert length(order) == length(Enum.uniq(order))
    end
  end

  # -- helpers ----------------------------------------------------------------

  defp header(xid, err), do: <<xid::32-signed, 1::64-signed, err::32-signed>>

  # Build the fake server's responses for a whole published cluster: the children of
  # /helios/nodes, each node's JSON document, and the desired state.
  defp cluster_responses(documents, desired) do
    names = documents |> Map.keys() |> Enum.sort()

    children =
      {:ok, <<length(names)::32-signed>> <> Enum.map_join(names, &Client.encode_string/1)}

    documents
    |> Enum.map(fn {name, document} ->
      {{4, "/helios/nodes/" <> name},
       {:ok, Client.encode_buffer(Jason.encode!(document)) <> @stat}}
    end)
    |> Map.new()
    |> Map.put({8, "/helios/nodes"}, children)
    |> Map.put({4, "/cluster_state"}, {:ok, Client.encode_buffer(desired) <> @stat})
  end

  defp start_client(opts) do
    opts =
      opts
      |> Keyword.put_new(:hosts, ["127.0.0.1"])
      |> Keyword.put_new(:connect_timeout_ms, 1_000)
      |> Keyword.put_new(:operation_timeout_ms, 2_000)
      |> Keyword.put(:name, nil)

    start_supervised!({Client, opts})
  end

  # A port nothing is listening on: bound to learn a free number, then released.
  defp closed_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end

  defp eventually(fun, attempts \\ 50)
  defp eventually(_fun, 0), do: false

  defp eventually(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(100)
      eventually(fun, attempts - 1)
    end
  end

  # -- fake ZooKeeper ---------------------------------------------------------
  #
  # `responses` maps {opcode, path} to {:ok, payload_after_the_reply_header} or
  # {:error, code}. Anything unmapped answers NoNode, which is what a real ensemble does
  # for a tree that has not been created yet.

  defp start_fake_zk(responses, observer \\ nil) do
    {:ok, listen} =
      :gen_tcp.listen(0, [
        :binary,
        {:packet, 4},
        {:active, false},
        {:reuseaddr, true},
        {:ip, {127, 0, 0, 1}}
      ])

    {:ok, port} = :inet.port(listen)
    server = spawn(fn -> accept_loop(listen, responses, observer) end)

    on_exit(fn ->
      Process.exit(server, :kill)
      :gen_tcp.close(listen)
    end)

    port
  end

  defp accept_loop(listen, responses, observer) do
    case :gen_tcp.accept(listen) do
      {:ok, socket} ->
        handshake(socket, responses, observer)
        accept_loop(listen, responses, observer)

      {:error, _reason} ->
        :ok
    end
  end

  defp handshake(socket, responses, observer) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, <<_protocol::32-signed, _zxid::64-signed, timeout::32-signed, _rest::binary>>} ->
        response =
          <<0::32-signed, timeout::32-signed, 0x1234::64-signed>> <>
            Client.encode_buffer(:binary.copy(<<9>>, 16))

        :gen_tcp.send(socket, response)
        if observer, do: send(observer, {:zk_connected, self()})
        serve(socket, responses, observer)

      _other ->
        :gen_tcp.close(socket)
    end
  end

  defp serve(socket, responses, observer) do
    # A hang-up request from the test takes priority over anything on the wire. It is
    # acknowledged so the test can act on a socket that is definitely closed.
    receive do
      :hang_up ->
        :gen_tcp.close(socket)
        if observer, do: send(observer, :zk_hung_up)
    after
      0 -> read_request(socket, responses, observer)
    end
  end

  defp read_request(socket, responses, observer) do
    case :gen_tcp.recv(socket, 0, 100) do
      {:ok, <<xid::32-signed, opcode::32-signed, payload::binary>>} ->
        if observer, do: send(observer, {:zk_request, xid, opcode, request_path(opcode, payload)})

        case opcode do
          -11 ->
            :gen_tcp.close(socket)

          11 ->
            :gen_tcp.send(socket, <<-2::32-signed, 0::64-signed, 0::32-signed>>)
            serve(socket, responses, observer)

          _operation ->
            path = request_path(opcode, payload)

            # Every reply is preceded by a watch notification, so the client's skipping
            # of xid -1 is exercised on every single request.
            :gen_tcp.send(
              socket,
              <<-1::32-signed, 0::64-signed, 0::32-signed, 3::32-signed, 3::32-signed>> <>
                Client.encode_string(path)
            )

            case Map.get(responses, {opcode, path}, {:error, -101}) do
              {:ok, body} ->
                :gen_tcp.send(socket, <<xid::32-signed, 1::64-signed, 0::32-signed>> <> body)

              {:error, code} ->
                :gen_tcp.send(socket, <<xid::32-signed, 1::64-signed, code::32-signed>>)
            end

            serve(socket, responses, observer)
        end

      {:error, :timeout} ->
        serve(socket, responses, observer)

      {:error, _closed} ->
        :gen_tcp.close(socket)
    end
  end

  defp request_path(11, _payload), do: nil

  defp request_path(_opcode, payload) do
    {path, _offset} = Client.decode_string(payload, 0)
    path
  end
end
