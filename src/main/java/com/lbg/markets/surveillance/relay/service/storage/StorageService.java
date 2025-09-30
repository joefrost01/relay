package com.lbg.markets.surveillance.relay.service.storage;

import com.lbg.markets.surveillance.relay.model.FileTransfer;

import java.io.IOException;
import java.nio.file.Path;

public interface StorageService {
    void uploadFile(FileTransfer transfer, Path sourcePath) throws IOException;

    boolean exists(String path) throws IOException;

    void deleteFile(String path) throws IOException;
}