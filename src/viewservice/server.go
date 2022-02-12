
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
/* 

TODO: 
1. Refactor to code to only keep track of acked, return, and current


*/

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

func (vs *ViewServer) getNextBackup() string {
	return vs.views.getNextView().Backup
}

func (vs *ViewServer) getNextPrimary() string {
	return vs.views.getNextView().Primary
}

func (vs *ViewServer) getLastViewToPrimary() *View {
	return vs.views.getReturnView()
}

// this function assumes that vs's primary server crashed
func (vs *ViewServer) handlePrimaryCrash(crashedArgs *PingArgs) {
	// log.Printf("handlePrimaryCrash")
	if vs.getNextBackup() == "" {
		if len(vs.serverStatus) == 0 {
			vs.views.emplaceNextView(crashedArgs.Me, "")
		} else { // else the service fail
			vs.dead = true
		} 
	} else {
		newBackup := vs.popIdleServer()
		if newBackup == "" {
			vs.views.emplaceNextView(vs.getNextBackup(), crashedArgs.Me)
		} else {
			vs.views.emplaceNextView(vs.getNextBackup(), newBackup)
		}
	}
}

func (vs *ViewServer) handleBackupCrash(crashedArgs *PingArgs) {
	// TODO
	// log.Printf("handleBackupCrash")
	newBackup := vs.popIdleServer()
	if newBackup == "" {
		vs.views.emplaceNextView(vs.getNextPrimary(), crashedArgs.Me)
	} else {
		vs.views.emplaceNextView(vs.getNextPrimary(), newBackup)
	}
		
}

func (vs *ViewServer) updatePing(args *PingArgs) {
	// log.Printf("updatePing")
	vs.serverStatus[args.Me] = time.Now()
}

func (vs *ViewServer) updateIdleServers(args *PingArgs) {
	// log.Printf("updateIdleServers")
	vs.idleServers[args.Me] = true
}

//
// server Ping RPC handler. 
// return view
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  // Your code here.
  // log.Printf("[Ping: start]")
	// args.Printf()
	// vs.views.Printf()

	if (args.Viewnum > vs.getLastViewToPrimary().Viewnum) {
		return fmt.Errorf("[Ping] Viewnum from Ping is more than the latest Viewnum sent to the primary server")
	}

	// 1. update view
	// if the last Ping primary server sent viewserver a signal
	if args.Me == vs.getLastViewToPrimary().Primary || 
		args.Me == vs.views.getACKedView().Primary {
		// the primary server has crashed
		if args.Viewnum == 0 {
			vs.handlePrimaryCrash(args)
			vs.views.updateACK(vs.views.getReturnView().Viewnum)
		} else {
			// ACK
			// log.Printf("[Ping] Update ACK")
			vs.views.updateACK(args.Viewnum)
		}

	// it's another server
	} else if args.Me == vs.getNextBackup() {
		if args.Viewnum == 0 {
			vs.handleBackupCrash(args)
		}

	// at this point the new args are coming from a new server or it is just idle
	} else if vs.getNextPrimary() == "" {
		// log.Printf("[Ping] emplace new primary")
		vs.views.emplaceNextView(args.Me, "")
	} else if vs.getNextBackup() == "" {
		// log.Printf("[Ping] emplace new backup")
		vs.views.emplaceNextView(vs.getNextPrimary(), args.Me)
	} else {
		// log.Printf("[Ping] update idle servers")
		vs.updateIdleServers(args)
	}

	// 2. update serverstatus
	vs.updatePing(args)

	// 3. return
	reply.View = *vs.getLastViewToPrimary()
	// vs.views.Printf()
  // log.Printf("[Ping: end]")
  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.
	reply.View = *vs.getLastViewToPrimary()

	// vs.views.Printf()
  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

  // Your code here.
	// log.Printf("[tick: start]")
	vs.views.Printf()
	if vs.isForzen(vs.getNextBackup()) {
		vs.views.emplaceNextView(vs.views.getNextView().Primary, vs.popIdleServer())
	}
	if vs.isForzen(vs.getNextPrimary()) {
		vs.views.emplaceNextView(vs.getNextBackup(), vs.popIdleServer())
	}
	// log.Printf("[tick: end]")
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
