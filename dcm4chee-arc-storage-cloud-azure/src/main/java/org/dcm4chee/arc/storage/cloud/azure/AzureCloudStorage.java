/*
 * *** BEGIN LICENSE BLOCK *****
 * Version: MPL 1.1/GPL 2.0/LGPL 2.1
 *
 * The contents of this file are subject to the Mozilla Public License Version
 * 1.1 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the
 * License.
 *
 * The Original Code is part of dcm4che, an implementation of DICOM(TM) in
 * Java(TM), hosted at https://github.com/dcm4che.
 *
 * The Initial Developer of the Original Code is
 * Kaiko BV
 * Portions created by the Initial Developer are Copyright (C) 2023
 * the Initial Developer. All Rights Reserved.
 *
 * Contributor(s):
 * See @authors listed below
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the MPL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the MPL, the GPL or the LGPL.
 *
 * *** END LICENSE BLOCK *****
 */

package org.dcm4chee.arc.storage.cloud.azure;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.NoSuchFileException;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.dcm4che3.net.Device;
import org.dcm4che3.util.AttributesFormat;
import org.dcm4chee.arc.conf.StorageDescriptor;
import org.dcm4chee.arc.metrics.MetricsService;
import org.dcm4chee.arc.storage.AbstractStorage;
import org.dcm4chee.arc.storage.ReadContext;
import org.dcm4chee.arc.storage.WriteContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobServiceVersion;
import com.azure.storage.blob.models.AccessTier;

/**
 * @author Daniil Trishkin <daniil@kaiko.ai>
 * @since March 2023
 */
public class AzureCloudStorage extends AbstractStorage {
    public final static String emailsProperty = "notification.emails";

    private static final Logger LOG = LoggerFactory.getLogger(AzureCloudStorage.class);
    private static final String DEFAULT_API_VERSION = "V2021_12_02";
    private static final String DEFAULT_CONTAINER = "dcm4chee";
    private static final String DEFAULT_TIER = "HOT";
    private final Device device;
    private final AttributesFormat pathFormat;
    private final String container;
    private final AccessTier tier;
    private final boolean synchronizeUpload;
    private BlobServiceVersion apiVersion;
    private BlobServiceClient blobServiceClient;
    private BlobContainerClient containerClient;

    @Override
    public WriteContext createWriteContext() {
        return new AzureCloudWriteContext(this);
    }

    protected AzureCloudStorage(StorageDescriptor descriptor, MetricsService metricsService, Device device) {
        super(descriptor, metricsService);
        this.device = device;

        synchronizeUpload = Boolean.parseBoolean(descriptor.getProperty("synchronizeUpload", null));
        pathFormat = new AttributesFormat(descriptor.getProperty("pathFormat", DEFAULT_PATH_FORMAT));
        container = descriptor.getProperty("container", DEFAULT_CONTAINER);
        tier = AccessTier.fromString(descriptor.getProperty("tier", DEFAULT_TIER).strip().toUpperCase());
        try {
            String v = descriptor.getProperty("apiVersion", DEFAULT_API_VERSION).strip().toUpperCase().replace('-',
                    '_');
            v = v.charAt(0) != 'V' ? 'V' + v : v;
            apiVersion = BlobServiceVersion.valueOf(v);
        } catch (EnumConstantNotPresentException e) {
            apiVersion = BlobServiceVersion.valueOf(DEFAULT_API_VERSION);
            LOG.error("API version is not available", e);
        }

        String endpoint = descriptor.getStorageURI().getSchemeSpecificPart();
        DefaultAzureCredential defaultCredential = new DefaultAzureCredentialBuilder().build();
        blobServiceClient = new BlobServiceClientBuilder()
                .serviceVersion(apiVersion)
                .credential(defaultCredential)
                .endpoint(endpoint)
                .buildClient();
        containerClient = blobServiceClient.getBlobContainerClient(container);
    }

    @Override
    protected Logger log() {
        return LOG;
    }

    @Override
    protected OutputStream openOutputStreamA(final WriteContext wc) throws IOException {
        final PipedInputStream in = new PipedInputStream();
        PipedOutputStream out = new PipedOutputStream(in);
        FutureTask<Void> task = new FutureTask<>(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {
                    copyA(in, wc);
                } finally {
                    in.close();
                }
                return null;
            }
        });
        ((AzureCloudWriteContext) wc).setUploadTask(task);
        device.execute(task);
        return out;
    }

    @Override
    protected void copyA(InputStream in, WriteContext wc) throws IOException {
        if (synchronizeUpload) {
            synchronized (descriptor) {
                upload(in, wc);
            }
        } else {
            upload(in, wc);
        }
    }

    @Override
    protected void copyA(ReadContext rc, WriteContext wc) throws IOException {
        if (synchronizeUpload) {
            synchronized (descriptor) {
                upload(rc, wc);
            }
        } else {
            upload(rc, wc);
        }
    }

    @Override
    protected void afterOutputStreamClosed(WriteContext wc) throws IOException {
        FutureTask<Void> task = ((AzureCloudWriteContext) wc).getUploadTask();
        try {
            task.get();
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        } catch (ExecutionException e) {
            Throwable c = e.getCause();
            if (c instanceof IOException)
                throw (IOException) c;
            throw new IOException("Upload failed", c);
        }
    }

    private void upload(InputStream in, WriteContext wc) throws IOException {
        String storagePath = pathFormat.format(wc.getAttributes());
        BlobClient blobClient = containerClient.getBlobClient(storagePath);
        if (!containerClient.exists())
            containerClient.create();
        BufferedInputStream bis = new BufferedInputStream(in);
        blobClient.upload(bis, true);
        blobClient.setAccessTier(tier);
        wc.setStoragePath(storagePath);
    }

    private void upload(ReadContext rc, WriteContext wc) throws IOException {
        String storagePath = pathFormat.format(wc.getAttributes());
        BlobClient blobClient = containerClient.getBlobClient(storagePath);
        if (!containerClient.exists())
            containerClient.create();
        String srcContainer = rc.getStorage().getStorageDescriptor().getProperty("container", DEFAULT_CONTAINER);
        String srcStoragePath = rc.getStoragePath();
        String endpoint = rc.getStorage().getStorageDescriptor().getStorageURI().getSchemeSpecificPart();
        blobClient.beginCopy(
                String.join("/", endpoint, srcContainer, srcStoragePath), null,
                tier, null, null, null, null);
        blobClient.setMetadata(Collections.singletonMap(emailsProperty,
                wc.getAttributes().getProperty(emailsProperty, null).toString()));
        wc.setStoragePath(storagePath);
    }

    @Override
    protected InputStream openInputStreamA(ReadContext rc) throws IOException {
        BlobClient blobClient = containerClient.getBlobClient(rc.getStoragePath());
        if (!blobClient.exists())
            throw objectNotFound(rc.getStoragePath());
        return blobClient.openInputStream();
    }

    @Override
    public boolean exists(ReadContext rc) {
        BlobClient blobClient = containerClient.getBlobClient(rc.getStoragePath());
        return blobClient.exists();
    }

    @Override
    public long getContentLength(ReadContext rc) throws IOException {
        BlobClient blobClient = containerClient.getBlobClient(rc.getStoragePath());
        if (!blobClient.exists())
            throw objectNotFound(rc.getStoragePath());

        return blobClient.getProperties().getBlobSize();
    }

    @Override
    public byte[] getContentMD5(ReadContext rc) throws IOException {
        BlobClient blobClient = containerClient.getBlobClient(rc.getStoragePath());
        if (!blobClient.exists())
            throw objectNotFound(rc.getStoragePath());
        return blobClient.getProperties().getContentMd5();
    }

    @Override
    protected void deleteObjectA(String storagePath) throws IOException {
        BlobClient blobClient = containerClient.getBlobClient(storagePath);
        if (!blobClient.exists())
            throw objectNotFound(storagePath);
        blobClient.delete();
    }

    private IOException objectNotFound(String storagePath) {
        return new NoSuchFileException("No Object[" + storagePath
                + "] in Container[" + container
                + "] on " + getStorageDescriptor());
    }
}
