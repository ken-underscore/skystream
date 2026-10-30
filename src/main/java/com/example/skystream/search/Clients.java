package com.example.skystream.search;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import org.springframework.http.server.reactive.ServerHttpRequest;

import io.netty.util.NetUtil;

/** Who sent a request, as the search rate limit counts clients. */
final class Clients {
    private Clients() {}

    static String of(ServerHttpRequest request) {
        return of(request.getRemoteAddress(), request.getHeaders().getOrEmpty("X-Forwarded-For"));
    }

    /**
     * The client of a request from {@code peer}. When the peer is a proxy on this machine, the client is the last
     * address in {@code X-Forwarded-For}, the one that proxy added. Otherwise it's the peer: from anywhere else, those
     * headers, and {@code Forwarded}, could be forged. An IPv6 client counts by its /64, the block one host can usually
     * rotate through.
     */
    static String of(InetSocketAddress peer, List<String> forwardedFor) {
        InetAddress address = peer == null ? null : peer.getAddress();
        if (address != null && address.isLoopbackAddress()) {
            String[] entries = String.join(",", forwardedFor).split(",");
            // Parsed as a literal only: never looked up in DNS.
            InetAddress forwarded = NetUtil.createInetAddressFromIpAddressString(entries[entries.length - 1].strip());
            if (forwarded != null) address = forwarded;
        }
        // Only a test's request has no address.
        if (address == null) return "unknown";
        if (!(address instanceof Inet6Address)) return address.getHostAddress();
        byte[] block = Arrays.copyOf(address.getAddress(), 16);
        Arrays.fill(block, 8, 16, (byte) 0);
        try {
            return InetAddress.getByAddress(block).getHostAddress() + "/64";
        } catch (UnknownHostException e) {
            throw new IllegalStateException("16 bytes are an IPv6 address", e);
        }
    }
}
