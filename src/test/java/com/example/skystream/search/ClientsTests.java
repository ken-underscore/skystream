package com.example.skystream.search;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetSocketAddress;
import java.util.List;

import org.junit.jupiter.api.Test;

class ClientsTests {
    private static InetSocketAddress peer(String address) {
        return new InetSocketAddress(address, 50_000);
    }

    @Test
    void trustsTheLastForwardedAddressOnlyFromAProxyOnThisMachine() {
        assertEquals("198.51.100.7", Clients.of(peer("127.0.0.1"), List.of("203.0.113.9, 198.51.100.7")));
        assertEquals("198.51.100.7", Clients.of(peer("::1"), List.of("203.0.113.9", "198.51.100.7")));
        assertEquals("192.0.2.1", Clients.of(peer("192.0.2.1"), List.of("198.51.100.7")));
        assertEquals("192.0.2.1", Clients.of(peer("192.0.2.1"), List.of()));
    }

    @Test
    void fallsBackToThePeerWhenTheForwardedAddressIsntOne() {
        // Not looked up in DNS.
        assertEquals("127.0.0.1", Clients.of(peer("127.0.0.1"), List.of("localhost")));
        assertEquals("127.0.0.1", Clients.of(peer("127.0.0.1"), List.of("198.51.100.7, ")));
        assertEquals("127.0.0.1", Clients.of(peer("127.0.0.1"), List.of()));
        assertEquals("unknown", Clients.of(null, List.of("198.51.100.7")));
    }

    @Test
    void countsIpv6ClientsByTheirSlash64() {
        assertEquals("2001:db8:1:2::/64", Clients.of(peer("2001:db8:1:2:aaaa:bbbb:cccc:dddd"), List.of()));
        assertEquals("2001:db8:1:2::/64", Clients.of(peer("127.0.0.1"), List.of("2001:db8:1:2::1")));
    }
}
