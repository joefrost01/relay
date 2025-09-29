package com.lbg.markets.surveillance.relay.repository;

import com.lbg.markets.surveillance.relay.model.FileTransfer;

import java.util.List;

public interface FileTransferRepository {
    boolean tryAcquireLock(Long id, byte[] rowVersion);
    List<FileTransfer> findPendingTransfers(int limit);
    FileTransfer registerFileIfNew(String sourceSystem, String filename,
                                   String filePath, Long fileSize, String fileHash);
}

