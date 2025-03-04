package spdk

import (
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/backupstore/backupbackingimage"
	"github.com/longhorn/backupstore/common"
	commonbitmap "github.com/longhorn/go-common-libs/bitmap"
	commonnet "github.com/longhorn/go-common-libs/net"
	commonns "github.com/longhorn/go-common-libs/ns"
	commontypes "github.com/longhorn/go-common-libs/types"
	"github.com/longhorn/go-spdk-helper/pkg/nvme"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
)

type CreateBackupBackingImageParameters struct {
	BackupName        string
	BackingImageName  string
	Checksum          string
	DestURL           string
	CompressionMethod string
	ConcurrentLimit   int32
	Labels            []string
	Parameters        map[string]string
	BackingImage      *BackingImage
}

type BackupBackingImageStatus struct {
	sync.Mutex

	spdkClient *spdkclient.Client

	BackupName       string
	BackingImageName string
	BackingImage     *BackingImage

	fragmap *Fragmap
	IP      string
	Port    int32

	subsystemNQN   string
	controllerName string
	initiator      *nvme.Initiator
	devFh          *os.File
	executor       *commonns.Executor

	Error     string
	Progress  int
	BackupURL string
	State     common.ProgressState

	log logrus.FieldLogger
}

func NewBackupBackingImageStatus(spdkClient *spdkclient.Client, backupName, backingImageName string, backingImage *BackingImage, superiorPortAllocator *commonbitmap.Bitmap) (*BackupBackingImageStatus, error) {
	log := logrus.WithFields(logrus.Fields{
		"backingImage": backingImageName,
		"backupName":   backupName,
	})

	podIP, err := commonnet.GetIPForPod()
	if err != nil {
		return nil, err
	}

	port, _, err := superiorPortAllocator.AllocateRange(1)
	if err != nil {
		return nil, err
	}

	executor, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create executor")
	}

	backupBackingImageStatus := &BackupBackingImageStatus{
		spdkClient:       spdkClient,
		BackupName:       backupName,
		BackingImageName: backingImageName,
		BackingImage:     backingImage,
		State:            common.ProgressStateInProgress,

		IP:       podIP,
		Port:     port,
		executor: executor,
		log:      log,
	}

	err = backupBackingImageStatus.open()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open the backing image for reading the data")
	}

	return backupBackingImageStatus, nil
}

func (b *BackupBackingImageStatus) newFragmap() (*Fragmap, error) {
	lvsList, err := b.spdkClient.BdevLvolGetLvstore(b.BackingImage.LvsName, "")
	if err != nil {
		return nil, err
	}
	if len(lvsList) == 0 {
		return nil, errors.Errorf("cannot find lvs %v for volume %v backup creation", b.BackingImage.LvsName, b.BackingImageName)
	}
	lvs := lvsList[0]

	if lvs.ClusterSize == 0 || lvs.BlockSize == 0 {
		return nil, errors.Errorf("invalid cluster size %v block size %v lvs %v", lvs.ClusterSize, lvs.BlockSize, b.BackingImage.LvsName)
	}

	if (b.BackingImage.Size % lvs.ClusterSize) != 0 {
		return nil, errors.Errorf("backing image size %v is not multiple of cluster size %v", b.BackingImage.Size, lvs.ClusterSize)
	}

	numClusters := b.BackingImage.Size / lvs.ClusterSize

	return &Fragmap{
		ClusterSize: lvs.ClusterSize,
		NumClusters: numClusters,
		// Calculate the number of bytes in the fragmap required considering 8 bits per byte
		Map: make([]byte, (numClusters+7)/8),
	}, nil
}

func (b *BackupBackingImageStatus) open() error {
	b.Lock()
	defer b.Unlock()

	b.log.Info("Preparing snapshot lvol bdev for backup")
	frgmap, err := b.newFragmap()
	if err != nil {
		return err
	}
	b.fragmap = frgmap

	lvolName := b.BackingImage.Snapshot.Name

	b.BackingImage.Lock()
	defer b.BackingImage.Unlock()

	b.log.Infof("Exposing backing image snapshot lvol bdev %v", lvolName)
	subsystemNQN, controllerName, err := exposeSnapshotLvolBdev(b.spdkClient, b.BackingImage.LvsName, lvolName, b.IP, b.Port, b.executor)
	if err != nil {
		b.log.WithError(err).Errorf("Failed to expose backing image snapshot lvol bdev %v", lvolName)
		return errors.Wrapf(err, "failed to expose backing image snapshot lvol bdev %v", lvolName)
	}
	b.subsystemNQN = subsystemNQN
	b.controllerName = controllerName

	b.log.Infof("Creating NVMe initiator for backing image snapshot lvol bdev %v", lvolName)
	initiator, err := nvme.NewInitiator(lvolName, helpertypes.GetNQN(lvolName), nvme.HostProc)
	if err != nil {
		return errors.Wrapf(err, "failed to create NVMe initiator for backing image snapshot lvol bdev %v", lvolName)
	}
	if _, err := initiator.Start(b.IP, strconv.Itoa(int(b.Port)), false); err != nil {
		return errors.Wrapf(err, "failed to start NVMe initiator for backing image snapshot lvol bdev %v", lvolName)
	}
	b.initiator = initiator

	b.log.Infof("Opening NVMe device %v", b.initiator.Endpoint)
	devFh, err := os.OpenFile(b.initiator.Endpoint, os.O_RDONLY, 0666)
	if err != nil {
		return errors.Wrapf(err, "failed to open NVMe device %v for snapshot lvol bdev %v", b.initiator.Endpoint, lvolName)
	}
	b.devFh = devFh

	return nil
}

func (b *BackupBackingImageStatus) CloseFile() {
	b.Lock()
	defer b.Unlock()

	b.log.Infof("Closing NVMe device %v", b.initiator.Endpoint)
	if err := b.devFh.Close(); err != nil {
		b.log.WithError(err).Warnf("failed to close NVMe device %v", b.initiator.Endpoint)
	}

	b.log.Info("Stopping NVMe initiator")
	if _, err := b.initiator.Stop(true, true, true); err != nil {
		b.log.WithError(err).Warn("failed to stop NVMe initiator")
	}

	b.log.Info("Unexposing snapshot lvol bdev")
	lvolName := b.BackingImage.Snapshot.Name
	err := b.spdkClient.StopExposeBdev(helpertypes.GetNQN(lvolName))
	if err != nil {
		b.log.WithError(err).Warnf("failed to unexpose snapshot lvol bdev %v", lvolName)
	}
}

func (b *BackupBackingImageStatus) ReadFile(start int64, data []byte) error {
	b.Lock()
	defer b.Unlock()

	_, err := b.devFh.ReadAt(data, start)

	return err
}

func (b *BackupBackingImageStatus) UpdateBackupProgress(state string, progress int, backupURL string, err string) {
	b.Lock()
	defer b.Unlock()

	b.State = common.ProgressState(state)
	b.Progress = progress
	b.BackupURL = backupURL
	b.Error = err

	if b.Progress == 100 {
		b.State = common.ProgressStateComplete
	} else if b.Error != "" {
		b.State = common.ProgressStateError
	}
}

func (b *BackupBackingImageStatus) overlayFragmap(fragmap []byte, offset, size uint64) error {
	b.log.Debugf("Overlaying fragment map for offset %v size %v", offset, size)

	startBytes := int((offset / b.fragmap.ClusterSize) / 8)
	if startBytes+len(fragmap) > len(b.fragmap.Map) {
		return fmt.Errorf("invalid start bytes %v and fragmap length %v", startBytes, len(fragmap))
	}

	for i := 0; i < len(fragmap); i++ {
		b.fragmap.Map[startBytes+i] |= fragmap[i]
	}
	return nil
}

func (b *BackupBackingImageStatus) overlayFragmaps(lvol *Lvol) error {
	// Cluster size is 1 MiB by default, so each byte in fragmap represents 8 clusters.
	// Process 256 bytes at a time to reduce the number of calls.
	batchSize := 256 * (8 * b.fragmap.ClusterSize)

	offset := uint64(0)
	for {
		if offset >= b.BackingImage.Size {
			return nil
		}

		size := util.Min(batchSize, b.BackingImage.Size-offset)

		result, err := b.spdkClient.BdevLvolGetFragmap(lvol.UUID, uint64(offset), size)
		if err != nil {
			return err
		}

		fragmap, err := base64.StdEncoding.DecodeString(result.Fragmap)
		if err != nil {
			return err
		}

		err = b.overlayFragmap(fragmap, offset, size)
		if err != nil {
			return err
		}

		offset += size
	}
}

func (b *BackupBackingImageStatus) constructFragmap() error {
	lvol := b.BackingImage.Snapshot
	if lvol == nil {
		return errors.Errorf("failed to find snapshot lvol for backing image %v", b.BackingImageName)
	}

	b.log.Infof("Overlaying snapshot lvol bdev %v", lvol.Name)
	err := b.overlayFragmaps(lvol)
	if err != nil {
		return errors.Wrapf(err, "failed to overlay fragment map for backing image snapshot lvol bdev %v", lvol.Name)
	}

	return nil
}

func (b *BackupBackingImageStatus) constructMappings() *common.Mappings {
	b.log.Info("Constructing mappings")

	mappings := &common.Mappings{
		BlockSize: backupBlockSize,
	}

	mapping := common.Mapping{
		Offset: -1,
	}

	i := uint64(0)
	for i = 0; i < b.fragmap.NumClusters; i++ {
		if b.fragmap.Map.IsSet(uint64(i)) {
			offset := int64(i) * int64(b.fragmap.ClusterSize)
			offset -= (offset % backupBlockSize)
			if mapping.Offset != offset {
				mapping = common.Mapping{
					Offset: offset,
					Size:   backupBlockSize,
				}
				mappings.Mappings = append(mappings.Mappings, mapping)
			}
		}
	}

	b.log.Info("Constructed mappings")

	return mappings
}

func (b *BackupBackingImageStatus) CreateBackupBackingImageMappings() (*common.Mappings, error) {
	// Overlay the fragments of snapshots and store the result in the b.fragmap.Map
	if err := b.constructFragmap(); err != nil {
		return nil, errors.Wrapf(err, "failed to construct fragment map for backing image snapshot lvol")
	}

	return b.constructMappings(), nil
}

func DoBackupBackingImageInit(spdkClient *spdkclient.Client, params *CreateBackupBackingImageParameters, superiorPortAllocator *commonbitmap.Bitmap) (*backupbackingimage.BackupBackingImage, *BackupBackingImageStatus, *backupbackingimage.BackupConfig, error) {
	log := logrus.WithFields(logrus.Fields{"pkg": "backup"})
	log.Infof("Initializing backup for backup backing image %v ", params.BackupName)

	var err error

	if params.DestURL == "" {
		return nil, nil, nil, fmt.Errorf("missing input parameter")
	}

	var labelMap map[string]string
	if params.Labels != nil {
		labelMap, err = util.ParseLabels(params.Labels)
		if err != nil {
			err = errors.Wrapf(err, "failed to parse backup labels for backup backing image %v", params.BackupName)
			return nil, nil, nil, errors.Wrapf(err, "failed to parse backup labels for backup backing image %v", params.BackupName)
		}
	}

	backupStatus, err := NewBackupBackingImageStatus(spdkClient, params.BackupName, params.BackingImageName, params.BackingImage, superiorPortAllocator)
	if err != nil {
		return nil, nil, nil, errors.Wrapf(err, "failed to init BackupBackingImageStatus")
	}

	backupBackingImage := &backupbackingimage.BackupBackingImage{
		Name:              params.BackingImageName,
		Size:              int64(params.BackingImage.Size),
		Checksum:          params.Checksum,
		Labels:            labelMap,
		CompressionMethod: params.CompressionMethod,
		CreatedTime:       util.Now(),
	}

	backupConfig := &backupbackingimage.BackupConfig{
		Name:            params.BackingImageName,
		ConcurrentLimit: params.ConcurrentLimit,
		DestURL:         params.DestURL,
		Parameters:      params.Parameters,
	}
	return backupBackingImage, backupStatus, backupConfig, nil
}

func DoBackupCreate(
	backupBackingImage *backupbackingimage.BackupBackingImage,
	backupStatus *BackupBackingImageStatus,
	backupConfig *backupbackingimage.BackupConfig,
) error {
	log := logrus.WithFields(logrus.Fields{"pkg": "backup"})
	log.Infof("Creating backup backing image %v", backupStatus.BackingImageName)

	mappings, err := backupStatus.CreateBackupBackingImageMappings()
	if err != nil {
		return err
	}

	err = backupbackingimage.CreateBackingImageBackup(backupConfig, backupBackingImage, backupStatus, mappings)
	return err
}
