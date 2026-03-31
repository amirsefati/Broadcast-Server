package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const defaultAddr = "localhost:8080"

func main() {
	log.SetFlags(0)

	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "start":
		if err := runStart(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	case "connect":
		if err := runConnect(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	case "-h", "--help", "help":
		usage()
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Broadcast Server

Usage:
  broadcast-server start [options]
  broadcast-server connect [options]

Commands:
  start    Start the websocket broadcast server
  connect  Connect a CLI client to the server

Options:
`)
	startFlags := flag.NewFlagSet("start", flag.ContinueOnError)
	startAddr := startFlags.String("addr", defaultAddr, "server listen address")
	startPath := startFlags.String("path", "/ws", "websocket route")
	startFlags.SetOutput(os.Stderr)
	_ = startAddr
	_ = startPath
	startFlags.PrintDefaults()

	connectFlags := flag.NewFlagSet("connect", flag.ContinueOnError)
	connectAddr := connectFlags.String("addr", defaultAddr, "server address")
	connectPath := connectFlags.String("path", "/ws", "websocket route")
	connectName := connectFlags.String("name", "", "display name shown in outgoing messages")
	connectFlags.SetOutput(os.Stderr)
	_ = connectAddr
	_ = connectPath
	_ = connectName
	connectFlags.PrintDefaults()
}

func runStart(args []string) error {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	addr := fs.String("addr", defaultAddr, "server listen address")
	path := fs.String("path", "/ws", "websocket route")
	if err := fs.Parse(args); err != nil {
		return err
	}

	server := newBroadcastServer(*addr, *path)
	return server.run()
}

func runConnect(args []string) error {
	fs := flag.NewFlagSet("connect", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	addr := fs.String("addr", defaultAddr, "server address")
	path := fs.String("path", "/ws", "websocket route")
	name := fs.String("name", "", "display name shown in outgoing messages")
	if err := fs.Parse(args); err != nil {
		return err
	}

	return runClient(*addr, *path, *name)
}

type broadcastServer struct {
	addr     string
	wsPath   string
	upgrader websocket.Upgrader

	clientsMu sync.RWMutex
	clients   map[*client]struct{}
	nextID    uint64
}

type client struct {
	id   string
	conn *websocket.Conn
	mu   sync.Mutex
}

func newBroadcastServer(addr, wsPath string) *broadcastServer {
	return &broadcastServer{
		addr:   addr,
		wsPath: wsPath,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
		clients: make(map[*client]struct{}),
	}
}

func (s *broadcastServer) run() error {
	mux := http.NewServeMux()
	mux.HandleFunc(s.wsPath, s.handleWebSocket)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, "broadcast server is running; websocket endpoint: %s\n", s.wsPath)
	})

	httpServer := &http.Server{
		Addr:    s.addr,
		Handler: mux,
	}

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("server listening on http://%s%s", s.addr, s.wsPath)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	select {
	case <-sigCtx.Done():
		log.Println("shutdown signal received")
	case err := <-serverErr:
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s.closeAllClients()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown server: %w", err)
	}

	if err := <-serverErr; err != nil {
		return err
	}

	log.Println("server stopped")
	return nil
}

func (s *broadcastServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("upgrade failed: %v", err)
		return
	}

	clientID := fmt.Sprintf("client-%d", atomic.AddUint64(&s.nextID, 1))
	currentClient := &client{id: clientID, conn: conn}
	s.addClient(currentClient)
	log.Printf("%s connected", clientID)

	defer func() {
		s.removeClient(currentClient)
		_ = currentClient.close()
		log.Printf("%s disconnected", clientID)
		s.broadcast([]byte(fmt.Sprintf("[server] %s left", clientID)))
	}()

	s.broadcast([]byte(fmt.Sprintf("[server] %s joined", clientID)))

	for {
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				return
			}
			log.Printf("%s read error: %v", clientID, err)
			return
		}

		if messageType != websocket.TextMessage {
			continue
		}

		if len(payload) == 0 {
			continue
		}

		s.broadcast(payload)
	}
}

func (s *broadcastServer) addClient(currentClient *client) {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()
	s.clients[currentClient] = struct{}{}
}

func (s *broadcastServer) removeClient(currentClient *client) {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()
	delete(s.clients, currentClient)
}

func (s *broadcastServer) broadcast(message []byte) {
	s.clientsMu.RLock()
	clients := make([]*client, 0, len(s.clients))
	for currentClient := range s.clients {
		clients = append(clients, currentClient)
	}
	s.clientsMu.RUnlock()

	for _, currentClient := range clients {
		if err := currentClient.writeMessage(websocket.TextMessage, message); err != nil {
			log.Printf("broadcast write failed: %v", err)
			s.removeClient(currentClient)
			_ = currentClient.close()
		}
	}
}

func (s *broadcastServer) closeAllClients() {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()

	for currentClient := range s.clients {
		_ = currentClient.writeControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "server shutting down"),
			time.Now().Add(2*time.Second),
		)
		_ = currentClient.close()
		delete(s.clients, currentClient)
	}
}

func (c *client) writeMessage(messageType int, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return c.conn.WriteMessage(messageType, payload)
}

func (c *client) writeControl(messageType int, payload []byte, deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.WriteControl(messageType, payload, deadline)
}

func (c *client) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.Close()
}

func runClient(addr, wsPath, name string) error {
	wsURL := url.URL{
		Scheme: "ws",
		Host:   addr,
		Path:   wsPath,
	}

	log.Printf("connecting to %s", wsURL.String())
	conn, _, err := websocket.DefaultDialer.Dial(wsURL.String(), nil)
	if err != nil {
		return fmt.Errorf("connect websocket: %w", err)
	}
	defer conn.Close()

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	readErr := make(chan error, 1)
	go func() {
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				readErr <- err
				return
			}
			fmt.Println(string(message))
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)
	log.Println("connected; type messages and press Enter")
	for {
		select {
		case <-sigCtx.Done():
			_ = conn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, "client exiting"),
				time.Now().Add(2*time.Second),
			)
			return nil
		case err := <-readErr:
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return nil
			}
			return fmt.Errorf("read from server: %w", err)
		default:
		}

		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return fmt.Errorf("read stdin: %w", err)
			}
			_ = conn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, "stdin closed"),
				time.Now().Add(2*time.Second),
			)
			return nil
		}

		text := scanner.Text()
		if text == "" {
			continue
		}

		if name != "" {
			text = fmt.Sprintf("[%s] %s", name, text)
		}

		if err := conn.WriteMessage(websocket.TextMessage, []byte(text)); err != nil {
			return fmt.Errorf("send message: %w", err)
		}
	}
}
