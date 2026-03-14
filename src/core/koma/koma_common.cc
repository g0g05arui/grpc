#include <linux/kcm.h>
#define _GNU_SOURCE

#include <errno.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <string.h>
#include <sys/ioctl.h>

#include "koma_common.h"

struct kcm_attach;

int koma_init(void) {
  int komafd;
  komafd = socket(AF_KOMA, SOCK_DGRAM, KOMAPROTO_CONNECTED);
  if (komafd == -1){
    perror("koma Failure: socket(AF_KOMA)");
  }
  return komafd;
}

int koma_attach(int komafd, int csock) {
  int error;
  struct kcm_attach attach_info;

  memset(&attach_info, 0, sizeof(attach_info));
  attach_info.fd = csock;
  attach_info.bpf_fd = 0;

  error = ioctl(komafd, SIOCKOMAATTACH, &attach_info);
  if (error == -1){
    perror("IOCTL ERROR: ioctl(SIOCKOMAATTACH)");
  }
  return error;
}

int koma_pull(int komafd) {
  int error;
  error = ioctl(komafd, SIOCKOMAPULL);
  if (error == -1){
    perror("IOCTL ERROR: ioctl(SIOCKOMAPULL)");
  }
  return error;
}
