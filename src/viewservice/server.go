
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string


  // Your declarations here.
	views *ViewQueue
	idleServers map[string]bool // key is server port
	serverStatus map[string]time.Time // server port -> last time pinged
}

func (vs *ViewServer) isForzen(server string) bool {
	lastPingTime, ok := vs.serverStatus[server]
	if ok {
		return lastPingTime.Add(PingInterval).Before(time.Now())
	}
	return false
}

func (vs *ViewServer) popIdleServer() string {
	if (len(vs.idleServers) > 0) {
		 for serverPort := range vs.idleServers {
			delete(vs.idleServers, serverPort)
			return serverPort
		 }
	}
	return ""
}

func (vs *ViewServer) getCurrBackup() string {
	return vs.views.getCurrView().Backup
}

func (vs *ViewServer) getCurrPrimary() string {
	return vs.views.getCurrView().Primary
}

func (vs *ViewServer) getReturnView() *View {
	return vs.views.getReturnView()
}

// this function assumes that vs's primary server crashed
func (vs *ViewServer) handlePrimaryCrash(crashedArgs *PingArgs) {
	log.Printf("handlePrimaryCrash")
	if vs.getCurrBackup() == "" {
		vs.views.emplaceCurrView(crashedArgs.Me, "")
	} else {
		newBackup := vs.popIdleServer()
		if newBackup == "" {
			vs.views.emplaceCurrView(vs.getCurrBackup(), crashedArgs.Me)
		} else {
			vs.views.emplaceCurrView(vs.getCurrBackup(), newBackup)
		}
	}
}

func (vs *ViewServer) handleBackupCrash(crashedArgs *PingArgs) {
	// TODO
	log.Printf("handleBackupCrash")
	newBackup := vs.popIdleServer()
	if newBackup == "" {
		vs.views.emplaceCurrView(vs.getCurrPrimary(), crashedArgs.Me)
	} else {
		vs.views.emplaceCurrView(vs.getCurrPrimary(), newBackup)
	}
		
}

func (vs *ViewServer) updatePing(args *PingArgs) {
	log.Printf("updatePing")
	vs.serverStatus[args.Me] = time.Now()
}

func (vs *ViewServer) updateIdleServers(args *PingArgs) {
	log.Printf("updateIdleServers")
	vs.idleServers[args.Me] = true
}

//
// server Ping RPC handler. 
// return view
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  // Your code here.
  log.Printf("[Ping: start]")
	args.Printf()
	vs.views.Printf()

	// 1. update view
	// vs doesn't have primary yet (so it shouldn't have backup either)
	if args.Me == vs.getReturnView().Primary {
		// the primary server has crashed
		if args.Viewnum == 0 {
			vs.handlePrimaryCrash(args)
			vs.views.updateACK(vs.views.getReturnView().Viewnum)
		} else {
			// ACK
			log.Printf("[Ping] Update ACK")
				vs.views.updateACK(args.Viewnum)
		}

		// the backup server has crashed
	} else if args.Me == vs.getCurrBackup() {
		if args.Viewnum == 0 {
			vs.handleBackupCrash(args)
		}

	// at this point the new args are coming from a new server or it is just idle
	} else if vs.getCurrPrimary() == "" {
		log.Printf("[Ping] emplace new primary")
		vs.views.emplaceCurrView(args.Me, "")
	} else if vs.getCurrBackup() == "" {
		log.Printf("[Ping] emplace new backup")
		vs.views.emplaceCurrView(vs.getCurrPrimary(), args.Me)
	} else {
		log.Printf("[Ping] update idle servers")
		vs.updateIdleServers(args)
	}

	// 2. update serverstatus
	vs.updatePing(args)

	// 3. return
	reply.View = *vs.getReturnView()
	vs.views.Printf()
  log.Printf("[Ping: end]")
  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.
	reply.View = *vs.getReturnView()

	vs.views.Printf()
  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

  // Your code here.
	log.Printf("[tick: start]")
	if vs.isForzen(vs.getCurrBackup()) {
		vs.views.emplaceCurrView(vs.views.getCurrView().Primary, vs.popIdleServer())
	}
	if vs.isForzen(vs.getCurrPrimary()) {
		vs.views.emplaceCurrView(vs.getCurrBackup(), vs.popIdleServer())
	}
	log.Printf("[tick: end]")
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.
	vs.views = makeViewQueue()
	vs.idleServers = make(map[string]bool)
	vs.serverStatus = make(map[string]time.Time)

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
