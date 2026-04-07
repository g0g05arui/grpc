#pragma once

#define NUM_KOMA_SOCKETS 8  // must match koma/koma.h

extern "C"{
  #include <linux/kcm.h>
  #ifndef AF_KOMA
  #define AF_KOMA 46
  #endif

  #ifndef KOMAPROTO_CONNECTED
  #define KOMAPROTO_CONNECTED 0
  #endif

  #ifndef SIOCKOMAATTACH
  #define SIOCKOMAATTACH (SIOCPROTOPRIVATE + 0)
  #endif

  #ifndef SIOCKOMAPULL
  #define SIOCKOMAPULL (SIOCPROTOPRIVATE + 3)
  #endif

  int koma_init(void);
  int koma_attach(int komafd, int csock);
  int koma_pull(int komafd);
}
