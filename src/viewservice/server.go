
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
/* 


immutable: LastViewToPrimary
mmutable: newView

*/

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string


  // Your declarations here.
	lastViewToPrimary *View
	newView *View
	ACKedViewNum uint
	idleServers map[string]bool // key is server port
	serverStatus map[string]time.Time // server port -> last time pinged
}

func (vs *ViewServer) PrintIdleServers() {
	log.Printf("Idle servers:")
	for key, _ := range vs.idleServers {
		log.Printf("%s, ", GetCleanName(key))
	}
	log.Printf("====")
}

func (vs *ViewServer) PrintViews() {
	log.Printf("lastViewToPrimary")
	if vs.lastViewToPrimary != nil {
		vs.lastViewToPrimary.Printf()
	} else {
		log.Printf("\t")
	}

	log.Printf("newView")
	if vs.newView != nil {
		vs.newView.Printf()
	} else {
		log.Printf("\t")
	}
	log.Printf("ACKed view: %d", vs.ACKedViewNum)
	log.Printf("--------------------------------")
}

func (vs *ViewServer) PushIdleServer(server string) {
	vs.idleServers[server] = true
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

/* 

Cases when a new view is needed: 
1. Primary didn't Ping in time
2. Backup didn't Ping in time
3. There is no primary
4. There is no backup 

*/
func (vs *ViewServer) BuildNewView(primary string, backup string) *View {
	newView := new(View)

	newView.Primary = primary
	newView.Backup = backup

	if vs.lastViewToPrimary == nil {
		newView.Viewnum = 1
	} else {
		newView.Viewnum = vs.lastViewToPrimary.Viewnum + 1 
	}
	return newView
}

func (vs *ViewServer) IsNew() bool {
	return vs.lastViewToPrimary == nil 
}

func (vs *ViewServer) BackupRestarted(args *PingArgs) bool {
	// primary server crashed before the next tick; no new primary has been assigned yet
	// if so, then there shouldn't be a new view or the new view has the same Primary
	return args.Viewnum == 0 && vs.lastViewToPrimary.Backup == args.Me
}

func (vs *ViewServer) IsPrimary(args *PingArgs) bool {
	// primary server crashed before the next tick; no new primary has been assigned yet
	// if so, then there shouldn't be a new view or the new view has the same Primary
	return vs.lastViewToPrimary.Primary == args.Me
}

func (vs *ViewServer) IsBackup(args *PingArgs) bool {
	return vs.lastViewToPrimary.Backup == args.Me
}


func (vs *ViewServer) HasNoBackup() bool {
	if vs.IsNew() {
		return false 
	} else {
		return vs.lastViewToPrimary.Backup == ""
	}
}

func (vs *ViewServer) updatePingTime(args *PingArgs) {
	vs.serverStatus[args.Me] = time.Now()
}

func (vs *ViewServer) updateLastViewToPrimary(newView *View) {
	if vs.lastViewToPrimary == nil ||  vs.ACKed(vs.lastViewToPrimary) {
			vs.lastViewToPrimary = newView
	}
}

func (vs *ViewServer) ACKLastView(args *PingArgs) {
	if !vs.IsNew() && args.Viewnum == vs.lastViewToPrimary.Viewnum {
		vs.ACKedViewNum = args.Viewnum
	}
}

func (vs *ViewServer) ACKableServer(server string) bool {
	return !vs.IsNew() && vs.lastViewToPrimary.Primary == server
}

func (vs *ViewServer) ACKed(view *View) bool {
	return view != nil && view.Viewnum == vs.ACKedViewNum
}

//
// server Ping RPC handler. 
// return view
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	// log.Printf("[Ping]: before")
	// args.Printf()
	// vs.PrintViews()

	if !vs.IsNew() && vs.lastViewToPrimary.Viewnum < args.Viewnum {
		return fmt.Errorf("large view number")
	}

	if vs.IsNew() { 
		// there is no view formed yet, change new view
		vs.ACKLastView(args)
		newView := vs.BuildNewView(args.Me, "")
		vs.updateLastViewToPrimary(newView)

	} else if vs.IsPrimary(args) {

		if args.Viewnum == 0 {// primary crashed before the tick

			if vs.ACKableServer(args.Me) { // old primary crashed 
				if vs.lastViewToPrimary.Backup == "" { // no candidate for primary
					// log.Printf("[Ping]: no candidate for primary in the lastViewToPrimary, DEAD")
					vs.dead = true
				} else { // there is backup 
					vs.PushIdleServer(args.Me)
					newView := vs.BuildNewView(vs.lastViewToPrimary.Backup, vs.popIdleServer())
					vs.ACKLastView(args)
					vs.updateLastViewToPrimary(newView)
				}
			}
		} else { // no primary is crashed 
			if vs.ACKableServer(args.Me) {
				vs.ACKLastView(args)
			}
		}

	} else if vs.IsBackup(args) || vs.HasNoBackup() {

		if args.Viewnum == 0 { // backup crashed before tick || a new machine with no backup
			if vs.IsBackup(args) {
				// log.Println("[Ping] Backup Restarted")
			} else {
				vs.PushIdleServer(args.Me)
				newView := vs.BuildNewView(vs.lastViewToPrimary.Primary, vs.popIdleServer());
				vs.updateLastViewToPrimary(newView)
				// log.Println("[Ping] No Backup")
			}
			// vs.PrintIdleServers()
		} else {
			// do nothing 
		}
	} else { // idle
		vs.PushIdleServer(args.Me)
		// vs.PrintIdleServers()
	}
	vs.updatePingTime(args)

	// 3. return
	if vs.lastViewToPrimary != nil {
		reply.View = *vs.lastViewToPrimary
	}

	// vs.PrintViews()
	// log.Printf("[Ping]: **************************************************")
  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.
	if vs.lastViewToPrimary != nil {
		reply.View = *vs.lastViewToPrimary	
	}
  return nil
}

func (vs *ViewServer) isForzen(server string) bool {
	lastPingTime, ok := vs.serverStatus[server]
	if ok {
		return lastPingTime.Add(PingInterval).Before(time.Now())
	}
	return false
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	// log.Printf("[tick: start]")
	// vs.PrintViews()
	if !vs.IsNew() && 
			vs.ACKed(vs.lastViewToPrimary) &&
			vs.isForzen(vs.lastViewToPrimary.Primary) {

		if vs.lastViewToPrimary.Backup == "" || vs.isForzen(vs.lastViewToPrimary.Backup) {
			return
			// log.Printf("[tick] No backup and the primary server hasn't responded")
		} else {
			// log.Printf("[tick] latest view primary is frozen, replacing it with a new one")
			// vs.PrintViews()
			newView := vs.BuildNewView(vs.lastViewToPrimary.Backup, vs.popIdleServer())
			vs.updateLastViewToPrimary(newView)
			// vs.PrintViews()
		}
	}

	if !vs.IsNew() && 
			vs.ACKed(vs.lastViewToPrimary) &&
			vs.isForzen(vs.lastViewToPrimary.Backup) {

		// log.Printf("[tick] latest view backup is frozen, replacing it with an old one")
		newView := vs.BuildNewView(vs.lastViewToPrimary.Primary, vs.popIdleServer())
		vs.updateLastViewToPrimary(newView)
	}
	// vs.PrintViews()
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
	vs.lastViewToPrimary = nil
	vs.newView = nil
	vs.ACKedViewNum = 0
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
