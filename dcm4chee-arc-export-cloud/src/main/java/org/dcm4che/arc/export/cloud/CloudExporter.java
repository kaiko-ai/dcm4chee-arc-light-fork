/*
 * **** BEGIN LICENSE BLOCK *****
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
 * **** END LICENSE BLOCK *****
 *
 */

package org.dcm4che.arc.export.cloud;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.dcm4che3.data.Tag;
import org.dcm4che3.net.ApplicationEntity;
import org.dcm4chee.arc.conf.ArchiveAEExtension;
import org.dcm4chee.arc.conf.ArchiveDeviceExtension;
import org.dcm4chee.arc.conf.Availability;
import org.dcm4chee.arc.conf.ExporterDescriptor;
import org.dcm4chee.arc.conf.StorageDescriptor;
import org.dcm4chee.arc.entity.Location;
import org.dcm4chee.arc.entity.Task;
import org.dcm4chee.arc.exporter.AbstractExporter;
import org.dcm4chee.arc.exporter.ExportContext;
import org.dcm4chee.arc.qmgt.Outcome;
import org.dcm4chee.arc.retrieve.RetrieveContext;
import org.dcm4chee.arc.retrieve.RetrieveService;
import org.dcm4chee.arc.storage.ReadContext;
import org.dcm4chee.arc.storage.Storage;
import org.dcm4chee.arc.storage.StorageFactory;
import org.dcm4chee.arc.storage.WriteContext;
import org.dcm4chee.arc.store.InstanceLocations;
import org.dcm4chee.arc.store.StoreService;
import org.dcm4chee.arc.store.StoreSession;
import org.dcm4chee.arc.store.UpdateLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Daniil Trishkin <daniil@kaiko.ai>
 * @since March 2022
 */
public class CloudExporter extends AbstractExporter {

    private static Logger LOG = LoggerFactory.getLogger(CloudExporter.class);

    private final static List<String> cloudSchemas = Arrays.asList("azureblob");

    private final RetrieveService retrieveService;
    private final StoreService storeService;
    private final StorageFactory storageFactory;

    public CloudExporter(ExporterDescriptor descriptor, RetrieveService retrieveService,
            StoreService storeService, StorageFactory storageFactory) {
        super(descriptor);
        this.retrieveService = retrieveService;
        this.storeService = storeService;
        this.storageFactory = storageFactory;
    }

    @Override
    public Outcome export(ExportContext exportContext) throws Exception {
        String studyIUID = exportContext.getStudyInstanceUID();
        try (RetrieveContext retrieveContext = retrieveService.newRetrieveContext(
                exportContext.getAETitle(),
                studyIUID,
                exportContext.getSeriesInstanceUID(),
                exportContext.getSopInstanceUID())) {
            retrieveContext.setHttpServletRequestInfo(exportContext.getHttpServletRequestInfo());
            String storageID = descriptor.getExportURI().getSchemeSpecificPart();
            ApplicationEntity ae = retrieveContext.getLocalApplicationEntity();
            StoreSession storeSession = storeService.newStoreSession(ae).withObjectStorageID(storageID);
            storeService.restoreInstances(
                    storeSession,
                    studyIUID,
                    exportContext.getSeriesInstanceUID(),
                    ae.getAEExtensionNotNull(ArchiveAEExtension.class).purgeInstanceRecordsDelay());
            if (!retrieveService.calculateMatches(retrieveContext))
                return new Outcome(Task.Status.WARNING, noMatches(exportContext));

            Storage storage = retrieveService.getStorage(storageID, retrieveContext);
            StorageDescriptor storageDescriptor = storage.getStorageDescriptor();
            String storageSchema = storageDescriptor.getStorageURI().getScheme();
            if (!cloudSchemas.contains(storageSchema)) {
                return new Outcome(Task.Status.WARNING, String.format("{} is not a cloud storage", storageID));
            }

            try {
                Set<String> seriesIUIDs = new HashSet<>();
                retrieveContext.setDestinationStorage(storageDescriptor);
                for (InstanceLocations instanceLocations : retrieveContext.getMatches()) {
                    // Checking for existing locations on the destination device
                    Map<Boolean, List<Location>> locationsOnStorageByStatusOK = instanceLocations.getLocations()
                            .stream()
                            .filter(l -> l.getStorageID().equals(storageID))
                            .collect(Collectors.partitioningBy(Location::isStatusOK));
                    if (!locationsOnStorageByStatusOK.get(Boolean.TRUE).isEmpty()) {
                        retrieveContext.setNumberOfMatches(retrieveContext.getNumberOfMatches() - 1);
                        continue;
                    }

                    // Getting all valid locations on the same cloud
                    List<Location> locations = getValidLocations(instanceLocations, storageSchema);
                    if (locations == null || locations.isEmpty()) {
                        throw new IOException("Failed to find location of " + instanceLocations);
                    }

                    WriteContext writeCtx = storage.createWriteContext();
                    writeCtx.setAttributes(instanceLocations.getAttributes());
                    writeCtx.setStudyInstanceUID(studyIUID);
                    Location location = null;
                    try {
                        LOG.debug("Start copying {} to {}:\n", instanceLocations, storageDescriptor);
                        location = copyTo(retrieveContext, instanceLocations, locations, storage, writeCtx);
                        storeService.replaceLocation(storeSession, instanceLocations.getInstancePk(),
                                location, locationsOnStorageByStatusOK.get(Boolean.FALSE));
                        storage.commitStorage(writeCtx);
                        retrieveContext.incrementCompleted();
                        LOG.debug("Finished copying {} to {}:\n", instanceLocations, storageDescriptor);
                        seriesIUIDs.add(instanceLocations.getAttributes().getString(Tag.SeriesInstanceUID));
                    } catch (Exception e) {
                        LOG.warn("Failed to copy {} to {}:\n", instanceLocations, storageDescriptor, e);
                        retrieveContext.incrementFailed();
                        retrieveContext.addFailedMatch(instanceLocations);
                        if (location != null)
                            try {
                                storage.revokeStorage(writeCtx);
                            } catch (Exception e2) {
                                LOG.warn("Failed to revoke storage", e2);
                            }
                    }
                }
                if (!seriesIUIDs.isEmpty()) {
                    storeService.addStorageID(studyIUID, storageID);
                    for (String seriesIUID : seriesIUIDs) {
                        storeService.scheduleMetadataUpdate(studyIUID, seriesIUID);
                    }
                }
                return new Outcome(retrieveContext.failed() > 0
                        ? Task.Status.FAILED
                        : Task.Status.COMPLETED,
                        outcomeMessage(exportContext, retrieveContext, retrieveContext.getDestinationStorage()));
            } finally {
                retrieveContext.getRetrieveService().updateLocations(retrieveContext);
            }
        }
    }

    private List<Location> getValidLocations(InstanceLocations instanceLocations, String cloud)
            throws IOException {
        ArchiveDeviceExtension arcDev = retrieveService.getArchiveDeviceExtension();
        Map<Availability, List<Location>> locationsByAvailability = instanceLocations.getLocations().stream()
                .filter(Location::isDicomFile)
                .filter(l -> arcDev.getStorageDescriptor(l.getStorageID()).getStorageURI().getScheme()
                        .equals(cloud))
                .collect(Collectors.groupingBy(
                        l -> arcDev.getStorageDescriptor(l.getStorageID()).getInstanceAvailability()));

        List<Location> locations = locationsByAvailability.get(Availability.ONLINE);
        if (locations == null)
            locations = locationsByAvailability.get(Availability.NEARLINE);
        if (locations == null)
            locations = locationsByAvailability.get(Availability.OFFLINE);

        return locations;
    }

    private Location copyTo(RetrieveContext retrieveContext, InstanceLocations instanceLocations,
            List<Location> locations, Storage storage, WriteContext wc) throws IOException {
        for (Location location : locations) {
            Storage sourceStorage = retrieveService.getStorage(location.getStorageID(), retrieveContext);
            ReadContext rc = sourceStorage.createReadContext();
            rc.setStoragePath(location.getStoragePath());
            rc.setStudyInstanceUID(instanceLocations.getAttributes().getString(Tag.StudyInstanceUID));
            try {
                storage.copy(rc, wc);
                return new Location.Builder()
                        .storageID(storage.getStorageDescriptor().getStorageID())
                        .storagePath(wc.getStoragePath())
                        .transferSyntaxUID(location.getTransferSyntaxUID())
                        .objectType(Location.ObjectType.DICOM_FILE)
                        .size(location.getSize())
                        .digest(location.getDigest())
                        .build();
            } catch (Exception e) {
                LOG.warn("Failed to copy {} from {}:\n{}", instanceLocations, location, e);
                retrieveContext.getUpdateLocations().add(
                        new UpdateLocation(instanceLocations, location, Location.Status.MISSING_OBJECT, null));
            }
        }

        throw new IOException("Failed to copy " + instanceLocations);
    }
}
