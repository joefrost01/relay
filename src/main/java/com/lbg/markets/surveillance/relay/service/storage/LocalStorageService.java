package com.lbg.markets.surveillance.relay.service.storage;

import com.lbg.markets.surveillance.relay.model.FileTransfer;
import io.quarkus.arc.profile.IfBuildProfile;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;

@ApplicationScoped
@IfBuildProfile("dev")
public class LocalStorageService implements StorageService {

    @ConfigProperty(name = "relay.storage.local.path", defaultValue = "./target/storage")
    String basePath;

    @Override
    public void uploadFile(FileTransfer transfer, Path sourcePath) throws IOException {
        Path targetPath = buildTargetPath(transfer);
        Files.createDirectories(targetPath.getParent());
        Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);

        transfer.gcsPath = targetPath.toString();
    }

    @Override
    public boolean exists(String path) {
        return Files.exists(Paths.get(path));
    }

    @Override
    public void deleteFile(String path) throws IOException {
        Files.deleteIfExists(Paths.get(path));
    }

    private Path buildTargetPath(FileTransfer transfer) {
        LocalDate now = LocalDate.now();
        return Paths.get(basePath)
                .resolve(transfer.sourceSystem)
                .resolve(String.valueOf(now.getYear()))
                .resolve(String.format("%02d", now.getMonthValue()))
                .resolve(String.format("%02d", now.getDayOfMonth()))
                .resolve(transfer.filename);
    }
}