package com.lbg.markets.surveillance.relay.service.storage;

import com.google.cloud.storage.*;
import com.lbg.markets.surveillance.relay.model.FileTransfer;
import io.quarkus.arc.profile.UnlessBuildProfile;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@ApplicationScoped
@UnlessBuildProfile("dev")
public class GcsStorageService implements StorageService {

    @ConfigProperty(name = "relay.gcs.bucket")
    String bucket;

    @ConfigProperty(name = "relay.gcs.project-id")
    String projectId;

    private Storage storage;

    @PostConstruct
    void init() {
        this.storage = StorageOptions.newBuilder()
                .setProjectId(projectId)
                .build()
                .getService();
    }

    @Override
    public void uploadFile(FileTransfer transfer, Path sourcePath) throws IOException {
        BlobId blobId = BlobId.of(bucket, buildGcsPath(transfer));
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

        byte[] bytes = Files.readAllBytes(sourcePath);
        storage.create(blobInfo, bytes);

        // Store the GCS URI
        transfer.gcsPath = String.format("gs://%s/%s", bucket, buildGcsPath(transfer));  // FIXED
    }

    @Override
    public boolean exists(String path) {
        // Parse GCS path
        if (path.startsWith("gs://")) {
            path = path.substring(5); // Remove gs://
            String[] parts = path.split("/", 2);
            if (parts.length == 2) {
                BlobId blobId = BlobId.of(parts[0], parts[1]);
                Blob blob = storage.get(blobId);
                return blob != null && blob.exists();
            }
        }
        return false;
    }

    @Override
    public void deleteFile(String path) {
        if (path.startsWith("gs://")) {
            path = path.substring(5);
            String[] parts = path.split("/", 2);
            if (parts.length == 2) {
                BlobId blobId = BlobId.of(parts[0], parts[1]);
                storage.delete(blobId);
            }
        }
    }

    private String buildGcsPath(FileTransfer transfer) {
        return String.format("%s/%tY/%<tm/%<td/%s",
                transfer.sourceSystem,
                java.util.Date.from(transfer.createdAt),
                transfer.filename);
    }
}