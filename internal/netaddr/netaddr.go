// Package netaddr finds the address of the host's primary network
// interface, the interface of its default route, for the heartbeat `ip`
// field.
package netaddr

import (
	"bufio"
	"net"
	"net/netip"
	"os"
	"strings"
)

// Snapshot is the routing and address state the selection reads.
type Snapshot struct {
	// DefaultIface4 is the interface of the IPv4 default route, "" if none.
	DefaultIface4 string
	// DefaultIface6 is the interface of the IPv6 default route, "" if none.
	DefaultIface6 string
	// Addrs lists each interface's addresses with their prefix lengths.
	Addrs map[string][]netip.Prefix
}

// Source reads a fresh Snapshot.
type Source interface {
	Snapshot() (Snapshot, error)
}

// SourceFunc adapts a function to Source.
type SourceFunc func() (Snapshot, error)

// Snapshot implements Source.
func (f SourceFunc) Snapshot() (Snapshot, error) { return f() }

// Primary returns the address to report: the IPv4 address of the IPv4
// default route's interface when there is one, otherwise the global IPv6
// address of the IPv6 default route's interface. ok is false when neither
// exists.
func Primary(s Snapshot) (addr netip.Addr, ok bool) {
	if s.DefaultIface4 != "" {
		for _, p := range s.Addrs[s.DefaultIface4] {
			a := p.Addr().Unmap()
			if a.Is4() && !a.IsLinkLocalUnicast() && !a.IsLoopback() && !a.IsUnspecified() {
				return a, true
			}
		}
	}
	if s.DefaultIface6 != "" {
		for _, p := range s.Addrs[s.DefaultIface6] {
			a := p.Addr()
			if a.Is6() && !a.Is4In6() && a.IsGlobalUnicast() {
				return a.WithZone(""), true
			}
		}
	}
	return netip.Addr{}, false
}

// System returns the Source that reads the running host's routing tables
// (/proc/net/route and /proc/net/ipv6_route) and interface addresses.
func System() Source {
	return SourceFunc(func() (Snapshot, error) {
		s := Snapshot{
			DefaultIface4: defaultRoute4("/proc/net/route"),
			DefaultIface6: defaultRoute6("/proc/net/ipv6_route"),
			Addrs:         map[string][]netip.Prefix{},
		}
		ifaces, err := net.Interfaces()
		if err != nil {
			return s, err
		}
		for _, ifc := range ifaces {
			addrs, err := ifc.Addrs()
			if err != nil {
				continue
			}
			for _, a := range addrs {
				ipn, ok := a.(*net.IPNet)
				if !ok {
					continue
				}
				ip, ok := netip.AddrFromSlice(ipn.IP)
				if !ok {
					continue
				}
				ones, _ := ipn.Mask.Size()
				s.Addrs[ifc.Name] = append(s.Addrs[ifc.Name], netip.PrefixFrom(ip.Unmap(), ones))
			}
		}
		return s, nil
	})
}

// defaultRoute4 returns the interface of the first IPv4 route with
// destination and mask 0.
func defaultRoute4(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Scan() // header
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) >= 8 && fields[1] == "00000000" && fields[7] == "00000000" {
			return fields[0]
		}
	}
	return ""
}

// defaultRoute6 returns the interface of the first IPv6 ::/0 route that
// is not the unreachable loopback default.
func defaultRoute6(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) >= 10 && fields[0] == strings.Repeat("0", 32) && fields[1] == "00" && fields[9] != "lo" {
			return fields[9]
		}
	}
	return ""
}
