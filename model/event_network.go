/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package model

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// Three costs followed from that, and none of them was visible in the response:

// ---------------------------------------------------------------------------------------
// The legacy webhook URL policy — ONE definition, for every layer that records the column
// ---------------------------------------------------------------------------------------

// MaxWebhookURLLength bounds blnk.event_subscribers.webhook_url, in bytes.
const MaxWebhookURLLength = 2048

// ValidateWebhookURL judges a legacy webhook URL against the single policy every layer
// that writes blnk.event_subscribers.webhook_url must apply.
//
// Parameters:
//   - raw string: the URL exactly as it will be stored. Not trimmed by this function.
//
// Returns:
//   - message string: a short caller-facing statement of what is wrong, echoing no
//     caller input.
//   - reason string: why, for the log or the error cause.
func ValidateWebhookURL(raw string) (message, reason string) {
	if strings.TrimSpace(raw) == "" {
		return "", ""
	}

	if raw != strings.TrimSpace(raw) {
		return "The webhook URL must not have surrounding whitespace",
			"a URL differing from another only by whitespace is a copy-paste artefact, and this " +
				"column is stored verbatim, so trimming it would persist a destination the caller " +
				"did not supply"
	}

	// LENGTH BEFORE PARSING. url.Parse on a 100 KB string is work done on a value that was
	// never going to be accepted, and the check is cheaper than the parse.
	if len(raw) > MaxWebhookURLLength {
		return "The webhook URL is too long",
			fmt.Sprintf(
				"the URL is %d bytes, over the %d byte maximum",
				len(raw), MaxWebhookURLLength,
			)
	}

	// THE PARSER'S ERROR IS NOT RETURNED. url.Parse quotes the input back, and the input is a
	// third party's endpoint that has no business in Blnk's error responses or logs.
	parsed, err := url.Parse(raw)
	if err != nil {
		return "The webhook URL is not a valid URL", "the value could not be parsed as a URL"
	}

	if parsed.Scheme != "https" {
		return "The webhook URL must use https",
			fmt.Sprintf(
				"scheme %q is not permitted; ledger and identity payloads must not be pushed in cleartext",
				parsed.Scheme,
			)
	}

	host := parsed.Hostname()
	if host == "" {
		return "The webhook URL must name a host", "the URL carries no host"
	}

	if internal := InternalWebhookDestinationReason(host); internal != "" {
		return "The webhook URL must not address an internal destination",
			fmt.Sprintf("host %q is refused: %s", host, internal)
	}

	return "", ""
}

// InternalWebhookDestinationReason reports why a host is an internal destination, or ""
// when it is not visibly internal.
//
// Parameters:
//   - host string: the hostname or literal address from the URL, without a port.
//
// Returns:
//   - string: a short reason, or "" when the host is acceptable.
func InternalWebhookDestinationReason(host string) string {
	if address := net.ParseIP(host); address != nil {
		switch {
		case address.IsLoopback():
			return "it is a loopback address, which would make Blnk call itself"
		case address.IsLinkLocalUnicast(), address.IsLinkLocalMulticast():
			return "it is a link-local address, the range the cloud metadata service lives on"
		case address.IsPrivate():
			return "it is a private address, which reaches services that trust the network rather than the caller"
		case address.IsUnspecified():
			return "it is the unspecified address"
		case address.IsInterfaceLocalMulticast(), address.IsMulticast():
			return "it is a multicast address"
		default:
			return ""
		}
	}

	lowered := strings.ToLower(host)
	switch {
	case lowered == "localhost", strings.HasSuffix(lowered, ".localhost"):
		return "it resolves to loopback"
	// .local is mDNS and .internal is the conventional private zone — metadata.google.internal is
	// one of the two best-known metadata endpoints.
	case strings.HasSuffix(lowered, ".local"), strings.HasSuffix(lowered, ".internal"):
		return "it is an internal-only name"
	case !strings.Contains(lowered, "."):
		return "it is unqualified, so it can only resolve inside this deployment"
	default:
		return ""
	}
}

// ----------------------------------------------------------------------------- Webhook
// destination policy
// -----------------------------------------------------------------------------

// nat64WellKnownPrefixLen is the length in bytes of the RFC 6052 well-known prefix
// (64:ff9b::/96 — twelve bytes) after which an embedded IPv4 address begins.
const nat64WellKnownPrefixLen = 12

// nat64WellKnownPrefix is the first twelve bytes of 64:ff9b::/96.
var nat64WellKnownPrefix = [nat64WellKnownPrefixLen]byte{
	0x00, 0x64, 0xff, 0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
}

// extraInternalRanges are the CIDRs the net.IP predicates do not classify but that are
// nonetheless unreachable on the public internet, each paired with the reason reported
// when an address falls inside it.
var extraInternalRanges = []struct {
	cidr   string
	reason string
	parsed *net.IPNet
}{
	{
		cidr: "100.64.0.0/10",
		reason: "it is in the carrier-grade NAT range, where cloud providers host " +
			"internal infrastructure such as the metadata proxy",
	},
	{cidr: "192.0.0.0/24", reason: "it is in the IETF protocol-assignment range, which is not publicly routable"},
	{cidr: "198.18.0.0/15", reason: "it is in the benchmarking range, which is not publicly routable"},
}

// init parses the extra ranges once, at package load, so the hot path never parses a
// CIDR string. A malformed constant here would be a programming error rather than a
// runtime condition, so parse failure panics at load rather than degrading the policy
// silently at send time — a deny-list that quietly stopped covering a range is the one
// failure mode this whole section exists to prevent.
func init() {
	for i := range extraInternalRanges {
		_, network, err := net.ParseCIDR(extraInternalRanges[i].cidr)
		if err != nil {
			panic("model: malformed internal destination CIDR " + extraInternalRanges[i].cidr + ": " + err.Error())
		}

		extraInternalRanges[i].parsed = network
	}
}

// InternalIPReason reports why an IP address is an internal destination, or "" when it
// is one Blnk may send to.
//
// Parameters:
//   - address net.IP: the address, as parsed.
//
// Returns:
//   - string: the reason, or "" when the address is acceptable.
func InternalIPReason(address net.IP) string {
	if address == nil {
		return "it is not a valid IP address"
	}

	// NAT64 first, and deliberately so: the embedded address is what the packet ultimately
	// reaches, and every predicate below would describe the wrapper as ordinary global
	// unicast.
	if embedded := nat64EmbeddedIPv4(address); embedded != nil {
		if reason := InternalIPReason(embedded); reason != "" {
			return "it embeds an internal IPv4 address in the NAT64 well-known prefix, and " + reason
		}
	}

	// Ordered so that the reason REPORTED is the accurate one, not merely a refusal. The
	// pairs overlap: 224.0.0.0/24 and ff02::/16 are both link-local AND multicast, and
	// describing a multicast group as "the cloud instance metadata endpoint" would send an
	// operator to investigate the wrong thing. Unicast link-local is checked on its own,
	// and every remaining multicast form — link-local, interface-local, global — falls to
	// the multicast arm.
	switch {
	case address.IsLoopback():
		return "it is a loopback address"
	case address.IsLinkLocalUnicast():
		return "it is a link-local address, which reaches the cloud instance metadata endpoint"
	case address.IsMulticast():
		return "it is a multicast address"
	case address.IsPrivate():
		return "it is a private address inside Blnk's own network"
	case address.IsUnspecified():
		return "it is the unspecified address"
	}

	for _, candidate := range extraInternalRanges {
		if candidate.parsed != nil && candidate.parsed.Contains(address) {
			return candidate.reason
		}
	}

	// The backstop. Anything that is not global unicast after the enumerated checks is
	// refused on the strength of not being an address the public internet can route.
	if !address.IsGlobalUnicast() {
		return "it is not a globally routable address"
	}

	return ""
}

// nat64EmbeddedIPv4 returns the IPv4 address embedded in an RFC 6052 well-known-prefix
// NAT64 address, or nil when the address is not one.
func nat64EmbeddedIPv4(address net.IP) net.IP {
	// Only a true 16-byte form can carry the prefix. To4 returning non-nil means the
	// value is an IPv4 address (or its mapped spelling), which cannot be NAT64.
	if address.To4() != nil {
		return nil
	}

	sixteen := address.To16()
	if sixteen == nil {
		return nil
	}

	for i := 0; i < nat64WellKnownPrefixLen; i++ {
		if sixteen[i] != nat64WellKnownPrefix[i] {
			return nil
		}
	}

	return net.IPv4(sixteen[12], sixteen[13], sixteen[14], sixteen[15])
}

// InternalDestinationReason reports why a hostname or IP literal is an internal
// destination, or "" when it is not.
//
// Parameters:
//   - host string: the hostname or IP literal, without a port.
//
// Returns:
//   - string: the reason, or "" when the host is acceptable.
func InternalDestinationReason(host string) string {
	if address := net.ParseIP(host); address != nil {
		return InternalIPReason(address)
	}

	lowered := strings.ToLower(host)

	if lowered == "localhost" || strings.HasSuffix(lowered, ".localhost") {
		return "it resolves to loopback"
	}

	// .local is mDNS and .internal is the conventional private zone — and the name
	// metadata.google.internal is one of the two best-known metadata endpoints.
	if strings.HasSuffix(lowered, ".local") || strings.HasSuffix(lowered, ".internal") {
		return "it is an internal-only hostname"
	}

	// An unqualified single-label name can only resolve through a local search domain,
	// which is by definition inside the network Blnk runs in.
	if !strings.Contains(lowered, ".") {
		return "it is an unqualified hostname that can only resolve inside Blnk's own network"
	}

	return ""
}

// OperatorOwnableInternalIP reports whether an internal address is one an operator can
// plausibly own and legitimately point a webhook at.
//
// Parameters:
//   - address net.IP: the address already known to be internal.
//
// Returns:
//   - bool: true when an explicit operator assertion may permit this address.
func OperatorOwnableInternalIP(address net.IP) bool {
	if address == nil {
		return false
	}

	// A NAT64-wrapped internal address is never operator-ownable. An operator who owns
	// 10.0.0.0/8 writes 10.x.y.z; reaching it through 64:ff9b:: is a bypass attempt
	// dressed as a global unicast address.
	if nat64EmbeddedIPv4(address) != nil {
		return false
	}

	return address.IsLoopback() || address.IsPrivate()
}
